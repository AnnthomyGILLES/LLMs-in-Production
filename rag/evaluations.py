import ast
import re

from config import config
from tools import call_openai

one_score_pattern = re.compile("\[\[(\d+\.?\d*)\]\]")
one_score_pattern_backup = re.compile("\[(\d+\.?\d*)\]")


def llm_grader(question, response) -> float:
    """
    Evaluates the quality of a response provided by an AI assistant to a given user question using a large language model (LLM) as a judge.

    The evaluation is based on various factors, including helpfulness, relevance, accuracy, depth, creativity, and level of detail.
    The function utilizes a predefined configuration for an LLM to judge the response, where the LLM acts as an impartial judge.

    Parameters:
    - question (str): The user's question to which the AI assistant has provided a response.
    - response (str): The response given by the AI assistant to the user's question.

    Returns:
    - float: A numerical rating between 0.0 and 1.0, representing the quality of the response. The rating is the LLM's
             evaluation score divided by 10, where a higher score indicates a better response quality.

    The function sends a request to an LLM (specified in the config) with instructions to evaluate the response.
    The evaluation includes a brief explanation followed by a numeric rating on a scale of 1 to 10.
    The function extracts this numeric rating using regular expressions and converts it to a float value between 0.0 and 1.0.

    If the LLM fails to provide a numeric rating, the function defaults to a rating of 0.

    Note:
    The function depends on an external LLM (here Openai) specified in a configuration and a pattern to extract the numeric rating.
    It is inspired by the methodology presented in a research paper exploring the use of LLMs as judges for evaluating
    other chat assistant models.
    """
    rating_response = call_openai(
        model=config["models"]["retrieval"],
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {
                "role": "user",
                "content": f"[Instruction]\nPlease act as an impartial judge and evaluate the quality of the response "
                f"provided by an AI assistant to the user question displayed below. Your evaluation should "
                f"consider factors such as the helpfulness, relevance, accuracy, depth, creativity, and "
                f"level of detail of the response. Begin your evaluation by providing a short explanation. "
                f"Be as objective as possible. After providing your explanation, you must rate the response "
                f'on a scale of 1 to 10 by strictly following this format: "[[rating]]", for example: '
                f'"Rating: [[5]]".\n\n[Question]\n{question}\n\n[The Start of Assistant\'s Answer]'
                f"\n{response}\n[The End of Assistant's Answer]",
            },
        ],
        temperature=0.0,
    )
    match = re.search(one_score_pattern, rating_response)
    if not match:
        match = re.search(one_score_pattern_backup, rating_response)

    if match:
        rating = ast.literal_eval(match.groups()[0])
    else:
        rating = 0

    return rating / 10.0


if __name__ == "__main__":
    question = "what are the strategies to mitigate hallucination in llm"
    response = """The strategies to mitigate hallucination in Large Language Models (LLMs)
include the creation of hybrid models that integrate various mitigation
approaches, reducing reliance on labeled data, and exploring unsupervised or
weakly supervised learning techniques. Additionally, there are strategies such
as model development through new decoding strategies, knowledge graph-based
optimizations, the addition of novel loss function components, and supervised
fine-tuning. Furthermore, there is a focus on addressing the nuances of
hallucination in LLMs through diverse array of strategies and the synthesis of
techniques to produce coherent and contextually relevant information while
demonstrating heightened awareness and mitigation of hallucinatory outputs"""
    print(llm_grader(question, response))
