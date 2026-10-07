"""The token counter, checked through a real LangChain chain with a fake chat model, so no
Gemini call is made. What matters is that the node tag reaches the callback and that a
missing usage_metadata never raises."""
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.prompts import ChatPromptTemplate

from shared.metrics import diagnostic_agent_llm_tokens_total
from worker.app.agent.usage import TokenUsageHandler, node_from_tags, node_tag


class FakeChatModel(BaseChatModel):
    """Replies with a message that carries the given usage_metadata (or none)."""

    usage: dict | None = None

    @property
    def _llm_type(self):
        return "fake"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        message = AIMessage(content="ok", usage_metadata=self.usage)
        return ChatResult(generations=[ChatGeneration(message=message)])


def tokens(node, direction):
    return diagnostic_agent_llm_tokens_total.labels(node=node, direction=direction)._value.get()


def chain_for(node, usage):
    llm = FakeChatModel(usage=usage, callbacks=[TokenUsageHandler()])
    prompt = ChatPromptTemplate.from_messages([("human", "{question}")])
    return (prompt | llm).with_config(tags=[node_tag(node)])


async def test_tokens_are_counted_under_the_nodes_tag():
    before_in, before_out = tokens("classification", "input"), tokens("classification", "output")
    chain = chain_for("classification", {"input_tokens": 120, "output_tokens": 30, "total_tokens": 150})

    await chain.ainvoke({"question": "hi"})

    assert tokens("classification", "input") - before_in == 120
    assert tokens("classification", "output") - before_out == 30


async def test_two_nodes_are_counted_separately():
    before_a, before_b = tokens("log_analysis", "input"), tokens("recovery_planning", "input")

    await chain_for("log_analysis", {"input_tokens": 10, "output_tokens": 1, "total_tokens": 11}).ainvoke({"question": "x"})
    await chain_for("recovery_planning", {"input_tokens": 70, "output_tokens": 5, "total_tokens": 75}).ainvoke({"question": "x"})

    assert tokens("log_analysis", "input") - before_a == 10
    assert tokens("recovery_planning", "input") - before_b == 70


async def test_a_reply_without_usage_metadata_counts_nothing_and_does_not_raise():
    before = tokens("classification", "input")

    result = await chain_for("classification", None).ainvoke({"question": "hi"})

    assert result.content == "ok"
    assert tokens("classification", "input") == before


async def test_an_untagged_call_goes_under_unknown():
    before = tokens("unknown", "output")
    llm = FakeChatModel(usage={"input_tokens": 4, "output_tokens": 2, "total_tokens": 6}, callbacks=[TokenUsageHandler()])

    await (ChatPromptTemplate.from_messages([("human", "{q}")]) | llm).ainvoke({"q": "x"})

    assert tokens("unknown", "output") - before == 2


def test_node_from_tags():
    assert node_from_tags(["seq:step:1", "node:classification"]) == "classification"
    assert node_from_tags(["something else"]) == "unknown"
    assert node_from_tags(None) == "unknown"


def test_a_failing_counter_is_swallowed(monkeypatch):
    class Boom:
        def labels(self, **_):
            raise RuntimeError("metrics broke")

    monkeypatch.setattr("worker.app.agent.usage.diagnostic_agent_llm_tokens_total", Boom())
    response = type("R", (), {"generations": [[ChatGeneration(message=AIMessage(
        content="x", usage_metadata={"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}))]]})()

    TokenUsageHandler().on_llm_end(response, tags=[node_tag("classification")])
