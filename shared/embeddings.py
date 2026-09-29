from langchain_google_genai import GoogleGenerativeAIEmbeddings
from shared.config import GOOGLE_API_KEY, GEMINI_EMBEDDING_MODEL
import numpy as np

#we target 768 dimensions even though gemini-embedding-001's NATIVE output is 3072. The model is trained with Matryoshka Representation Learning, which means the first N
#dimensions of the 3072-dim vector are independently meaningful, truncating to 768 gives ~0.26% MTEB quality loss while saving 4x on storage, index size, and retrieval latency.
#this constant is the single source of truth for the output dimension across this module. If we ever switch to native 3072 (or to gemini-embedding-2), this constant changes 
# AND the IncidentEmbedding.embedding column dimension changes AND the HNSW index gets rebuilt. All three have to move together, they're all baked into the same number.
EMBEDDING_DIM=768

#singleton client at module load, same lifetime pattern as the chat LLM client in worker/app/agent/nodes.py.
#GoogleGenerativeAIEmbeddings is stateless and safe to share, instantiating once avoids credential-loading overhead on every call.
#we do NOT pass task_type to the constructor. The library defaults to RETRIEVAL_QUERY for embed_query() and RETRIEVAL_DOCUMENT for embed_documents(),
#which is exactly the asymmetric task-type behavior we want. If we'd set task_type in the constructor, it would override BOTH methods to use that one type, wrong for retrieval workloads.
#also note: we do NOT pass output_dimensionality to the constructor. There's a known bug/gotcha where the constructor silently ignores this parameter, it MUST be passed per-call to embed_query/embed_documents.
#that's why every helper function below threads output_dimensionality through explicitly.
_embeddings_client=GoogleGenerativeAIEmbeddings(model=GEMINI_EMBEDDING_MODEL,google_api_key=GOOGLE_API_KEY)


def _l2_normalize(vec: list[float]) -> list[float]:
    """L2-normalize a single vector so its magnitude equals 1.

    Why this is necessary: gemini-embedding-001 auto-normalizes ONLY at the native 3072 dim.
    Any truncated dimension (including our 768) comes back un-normalized, which breaks cosine similarity assumptions in pgvector, the <=> operator returns cosine DISTANCE, 
    but cosine distance is only meaningful when both vectors are unit-length.
    Storing un-normalized vectors would mean retrieval scores get distorted by vector magnitude rather than reflecting pure angular similarity.
    The newer gemini-embedding-2 model normalizes truncated outputs automatically; this manual step goes away when we eventually upgrade.
    """
    arr=np.array(vec, dtype=np.float32)
    norm=np.linalg.norm(arr)
    if norm==0: #defensive: a zero vector would divide-by-zero. Shouldn't happen with real embedding output, but we guard against it.
        return arr.tolist()
    return (arr/norm).tolist()


def _l2_normalize_batch(vecs: list[list[float]]) -> list[list[float]]:
    """Batch L2-normalization. Same math as _l2_normalize, vectorized via numpy for speed when we have many chunks at indexing time."""
    arr=np.array(vecs, dtype=np.float32)
    norms=np.linalg.norm(arr, axis=1, keepdims=True)
    #replace zero norms with 1 to avoid divide-by-zero, a zero vector divided by 1 stays zero, which is the right degenerate behavior.
    norms[norms==0]=1.0
    return (arr/norms).tolist()


# Sync API, used by the runbook indexer (CLI script, no async context).

def embed_text(text: str) -> list[float]:
    """Embed a single query string. Returns a 768-dim L2-normalized vector.

    Use this in retrieval contexts: the underlying call uses task_type=RETRIEVAL_QUERY, which optimizes the embedding for matching against documents indexed with RETRIEVAL_DOCUMENT.
    Asymmetric task types are how Gemini's embedding model improves retrieval quality, using the wrong one degrades results noticeably.
    """
    #output_dimensionality MUST be on the call, not the constructor (see module-level note above).
    raw=_embeddings_client.embed_query(text, output_dimensionality=EMBEDDING_DIM)
    return _l2_normalize(raw)


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Batch-embed multiple document strings. Returns a list of 768-dim L2-normalized vectors.

    Use this in indexing contexts: the underlying call uses task_type=RETRIEVAL_DOCUMENT, which is the correct counterpart to RETRIEVAL_QUERY at query time.
    Batching matters here, the Gemini API supports up to 100 texts per call, and batching is materially faster than looping single calls.
    """
    raw=_embeddings_client.embed_documents(texts, output_dimensionality=EMBEDDING_DIM)
    return _l2_normalize_batch(raw)


# Async API, used by the retrieval node inside the LangGraph diagnostic flow.
# Same semantics as the sync versions, just awaitable so they don't block the async event loop.
async def embed_text_async(text: str) -> list[float]:
    """Async version of embed_text. Use this from inside async code (LangGraph nodes, FastAPI routes)."""
    raw=await _embeddings_client.aembed_query(text, output_dimensionality=EMBEDDING_DIM)
    return _l2_normalize(raw)


async def embed_texts_async(texts: list[str]) -> list[list[float]]:
    """Async version of embed_texts. Less common usage, most batch indexing happens in sync CLI scripts, but provided for symmetry."""
    raw=await _embeddings_client.aembed_documents(texts, output_dimensionality=EMBEDDING_DIM)
    return _l2_normalize_batch(raw)