"""Runbook indexer. Run it by hand with `python -m worker.app.agent.index_runbooks`.

It reads worker/app/agent/runbooks/*.md, splits each file by markdown header, embeds the
chunks, and replaces every source_type='runbook' row in incident_embeddings with the new
set.

Each run replaces the whole runbook index, so running it twice gives the same result. The
files on disk are the source of truth and the database is only an index of them.

Run it whenever a runbook is added, edited or deleted.
"""

from pathlib import Path
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from shared.config import DATABASE_URL_SYNC
from shared.embeddings import embed_texts
#we import the model directly. The __init__.py at control_plane/app/models/ registers all models with Base.metadata, so importing IncidentEmbedding alone is enough, 
# but we're using a sync Session here (not the async session from shared/db.py) since this is a one-shot CLI script and async overhead is wasted.
from control_plane.app.models.incident_embeddings import IncidentEmbedding

#RUNBOOKS_DIR is computed relative to this file's location so the script works regardless of where you invoke it from. Path(__file__) is this script; .parent is worker/app/agent/; .parent / "runbooks" is the directory.
RUNBOOKS_DIR=Path(__file__).parent/"runbooks"

#chunking config. Our runbooks have predictable structure (## What This Looks Like, ## Common Root Causes, etc.) so we split on the level-2 headers first to keep each
#section as its own logical chunk. Within sections, if any section is unusually long, we fall back to character-level splitting so we don't blow past embedding context limits or 
# produce one giant chunk that retrieves poorly.
#chunk_size=1500 chars is well under our ~500-token target (roughly 4 chars/token average for English). chunk_overlap=200 preserves context across split boundaries.
#these are deliberately generous, our sections are mostly small enough that the character-level splitter rarely activates. The overlap matters more when sections are large enough to split.
MARKDOWN_HEADERS_TO_SPLIT_ON=[("##", "section")] #we only care about level-2 headers. Level-1 (the runbook title) is the same per file and doesn't add retrieval signal.
CHUNK_SIZE=1500
CHUNK_OVERLAP=200


def _category_from_filename(filename: str) -> str:
    """Derive the FailureClassification category from the runbook filename.

    network.md -> 'network', partial_load.md -> 'partial_load', etc. The category goes into meta so the retrieval node can later filter or weight runbooks by category 
    if we want to. Keeping this as a small helper makes the convention explicit, if we ever add a runbook whose filename doesn't match a category Literal exactly, this is the one spot to handle that.
    """
    return filename.replace(".md", "")


def _normalize_section(section: str) -> str:
    """Normalize a section heading into something URL-fragment-friendly for source_ref.

    'What This Looks Like' -> 'what_this_looks_like'. Lowercase + spaces-to-underscores is enough; we don't need full slug rules here because the only consumer is human eyeballs reading citations.
    """
    return section.lower().replace(" ", "_")


def _chunk_runbook(filepath: Path) -> list[dict]:
    """Chunk a single runbook file. Returns a list of dicts with chunk_text, source_ref, and meta.

    Two-phase split:
      1. MarkdownHeaderTextSplitter parses the file, producing one Document per ## section with the section name in metadata.
      2. RecursiveCharacterTextSplitter then character-splits any section that exceeds CHUNK_SIZE, preserving the section metadata across the splits.

    For most of our runbooks, phase 2 is a no-op, sections are 100-200 words each. But the safety net matters: if someone later writes a verbose Examples section, retrieval doesn't break.
    """
    text=filepath.read_text(encoding="utf-8")
    category=_category_from_filename(filepath.name)

    #phase 1: split by ## headers. Each output Document has page_content (the section body) and metadata={"section": "What This Looks Like"} or similar.
    #note that the level-1 # header (the runbook title) is NOT in our headers_to_split_on, so it gets prepended to the first section's content. That's fine, it gives the first chunk some extra context.
    header_splitter=MarkdownHeaderTextSplitter(headers_to_split_on=MARKDOWN_HEADERS_TO_SPLIT_ON, strip_headers=False) 
    #strip_headers=False keeps "## Examples" inside the chunk content. This is deliberate, having the section name in the embedded text gives the embedding model a strong anchor for what kind of content it is.
    section_documents=header_splitter.split_text(text)

    #phase 2: char-level split for any oversized sections. RecursiveCharacterTextSplitter preserves the metadata field across splits, so we don't lose section context.
    char_splitter=RecursiveCharacterTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    chunks=char_splitter.split_documents(section_documents)

    out=[]
    for chunk in chunks:
        #fall back to 'unknown_section' if for some reason the metadata is missing, defensive, shouldn't happen with our well-structured runbooks.
        section=chunk.metadata.get("section", "unknown_section")
        out.append({"chunk_text": chunk.page_content,"source_ref": f"{filepath.name}#{_normalize_section(section)}",
                    "meta": {"category": category,"section": section, "filename": filepath.name,}})
    return out


def main():
    if not RUNBOOKS_DIR.exists():
        print(f"Runbooks directory not found: {RUNBOOKS_DIR}")
        return

    runbook_files=sorted(RUNBOOKS_DIR.glob("*.md"))
    if not runbook_files:
        print(f"No runbook files found in {RUNBOOKS_DIR}")
        return

    print(f"Found {len(runbook_files)} runbook files: {[f.name for f in runbook_files]}")

    #collect all chunks across all files BEFORE embedding. This lets us batch-embed everything in one or a small number of API calls instead of one call per file. Materially faster.
    all_chunks=[]
    for filepath in runbook_files:
        chunks=_chunk_runbook(filepath)
        all_chunks.extend(chunks)
        print(f"  {filepath.name}: {len(chunks)} chunks")

    print(f"\nTotal chunks to embed: {len(all_chunks)}")
    if not all_chunks: #shouldn't happen if the runbooks have any content, but guard against it.
        print("Nothing to index. Exiting.")
        return

    #batch-embed all chunks. The embeddings helper handles task_type=RETRIEVAL_DOCUMENT (correct for content being stored), output_dimensionality=768, and L2 normalization.
    print("Embedding chunks...")
    embeddings=embed_texts([c["chunk_text"] for c in all_chunks])
    print(f"Got {len(embeddings)} embedding vectors")

    #single transaction: clear existing runbook rows, insert all new rows. Wrapping in one transaction means a partial failure leaves the prior runbook index intact, never half-indexed state.
    engine=create_engine(DATABASE_URL_SYNC)
    with Session(engine) as session:
        try:
            #wipe ALL existing runbook rows. We don't try to be clever about diffing, runbooks are small enough that full replacement is simpler and bug-free.
            delete_result=session.execute(delete(IncidentEmbedding).where(IncidentEmbedding.source_type=="runbook"))
            print(f"Deleted {delete_result.rowcount} existing runbook rows")

            #insert new rows. tenant_id is NULL because runbooks are global; source_id is NULL because runbooks have no DB-side source row to reference.
            for chunk, embedding in zip(all_chunks, embeddings):
                row=IncidentEmbedding(source_type="runbook",source_id=None,source_ref=chunk["source_ref"],
                                       chunk_text=chunk["chunk_text"],embedding=embedding,tenant_id=None,meta=chunk["meta"],)
                session.add(row)
            session.commit()
            print(f"Inserted {len(all_chunks)} new runbook rows")
        except Exception as e:
            session.rollback()
            print(f"Indexing failed, rolled back: {e}")
            raise

    print("\nRunbook indexing complete.")


if __name__=="__main__":
    main()