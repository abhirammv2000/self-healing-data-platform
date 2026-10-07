"""Counts the tokens each agent node spends, as a Prometheus metric.

The handler is attached to the Gemini client in nodes.py. LangChain calls it after every
model call with the reply, and Gemini replies carry usage_metadata with the input and output
token counts. Each node's chain is tagged "node:<name>", and the handler reads that tag, so
the metric says which node spent the tokens.

It only counts tokens. Turning tokens into dollars needs the price of the model you run, so
that is left to a query or dashboard. For example:
    sum by (node) (increase(diagnostic_agent_llm_tokens_total[1d]))
"""
from langchain_core.callbacks import BaseCallbackHandler

from shared.metrics import diagnostic_agent_llm_tokens_total
from shared.observability import get_logger

log = get_logger(__name__)

NODE_TAG_PREFIX = "node:"


def node_tag(name):
    return NODE_TAG_PREFIX + name


def node_from_tags(tags):
    for tag in tags or []:
        if tag.startswith(NODE_TAG_PREFIX):
            return tag[len(NODE_TAG_PREFIX):]
    return "unknown"


class TokenUsageHandler(BaseCallbackHandler):
    def on_llm_end(self, response, *, tags=None, **kwargs):
        # A broken metric must never break a diagnosis, so nothing in here may raise.
        try:
            node = node_from_tags(tags)
            for generations in response.generations:
                for generation in generations:
                    usage = getattr(getattr(generation, "message", None), "usage_metadata", None)
                    if not usage:
                        continue
                    diagnostic_agent_llm_tokens_total.labels(node=node, direction="input").inc(
                        usage.get("input_tokens", 0))
                    diagnostic_agent_llm_tokens_total.labels(node=node, direction="output").inc(
                        usage.get("output_tokens", 0))
        except Exception as e:
            log.warning("token_usage_not_recorded", error=str(e))
