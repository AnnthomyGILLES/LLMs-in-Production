import os

from dotenv import load_dotenv
from trulens_eval import Feedback, Select, Tru
from trulens_eval.feedback import Groundedness
from trulens_eval.feedback.provider.openai import OpenAI as fOpenAI
from trulens_eval import TruCustomApp

import numpy as np

from generation import ResearchAssistant

load_dotenv()

# Initialize provider class
fopenai = fOpenAI(api_key=os.getenv("OPENAI_API"))

grounded = Groundedness(groundedness_provider=fopenai)

# Define a groundedness feedback function
f_groundedness = (
    Feedback(grounded.groundedness_measure_with_cot_reasons, name="Groundedness")
    .on(Select.RecordCalls.retrieve.rets.collect())
    .on_output()
    .aggregate(grounded.grounded_statements_aggregator)
)

# Question/answer relevance between overall question and answer.
f_qa_relevance = (
    Feedback(fopenai.relevance_with_cot_reasons, name="Answer Relevance")
    .on(Select.RecordCalls.retrieve.args.query)
    .on_output()
)

# Question/statement relevance between question and each context chunk.
f_context_relevance = (
    Feedback(fopenai.qs_relevance_with_cot_reasons, name="Context Relevance")
    .on(Select.RecordCalls.retrieve.args.query)
    .on(Select.RecordCalls.retrieve.rets.collect())
    .aggregate(np.mean)
)

if __name__ == "__main__":
    tru = Tru()

    rag = ResearchAssistant()
    tru_rag = TruCustomApp(
        rag,
        app_id="RAG v1",
        feedbacks=[f_groundedness, f_qa_relevance, f_context_relevance],
    )
    with tru_rag as recording:
        rag.process_query("what are the strategies to mitigate hallucination in llm?")
    tru.get_leaderboard(app_ids=["RAG v1"])
    tru.run_dashboard()
