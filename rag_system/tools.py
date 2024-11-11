import os
from typing import Union

import numpy as np
from dotenv import load_dotenv
from openai import __version__ as openai_version, OpenAI
from tqdm import tqdm

load_dotenv()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API"))


def word_wrap(string, n_chars=80):
    # Wrap a string at the next space after n_chars
    if len(string) < n_chars:
        return string
    else:
        return (
            string[:n_chars].rsplit(" ", 1)[0]
            + "\n"
            + word_wrap(string[len(string[:n_chars].rsplit(" ", 1)[0]) + 1 :], n_chars)
        )


def project_embeddings(embeddings, umap_transform):
    umap_embeddings = np.empty((len(embeddings), 2))
    for i, embedding in enumerate(tqdm(embeddings)):
        umap_embeddings[i] = umap_transform.transform([embedding])

    return umap_embeddings


def call_openai(
    messages,
    model,
    temperature=0.5,
    max_tokens=None,
    top_p=1.0,
    frequency_penalty=0.0,
    presence_penalty=0.0,
    n=1,
) -> Union[str, list[str]]:
    if openai_version.startswith("0."):
        completion = openai_client.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
            n=n,
        )
        if n == 1:
            return completion.choices[0].message["content"]
        else:
            return [c.message["content"] for c in completion.choices]
    else:
        completion = openai_client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
            n=n,
        )
        if n == 1:
            return completion.choices[0].message.content
        else:
            return [c.message.content for c in completion.choices]


if __name__ == "__main__":
    word_wrap("test")
