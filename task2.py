import matplotlib.pyplot as plt
import string

from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

import requests


def visualize_top_words(top_words: dict):
    top10 = dict(sorted(top_words.items(), key=lambda x: x[1], reverse=True)[:10])
    fig, ax = plt.subplots()
    ax.bar(top10.keys(), top10.values())
    ax.set_title("Top words")
    ax.set_xlabel("Word")
    ax.set_ylabel("Count")
    plt.show()


def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        return None


def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))


def map_function(word):
    return word, 1


def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


# MapReduce
def map_reduce(text, search_words=None):
    text = remove_punctuation(text)
    words = text.split()
    if search_words:
        words = [word for word in words if word in search_words]

    # Parallel mapping
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Parallel reduction
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


if __name__ == '__main__':
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)
    if text:
        result = map_reduce(text)
        print("Результат підрахунку слів:", result)
        visualize_top_words(result)
    else:
        print("Помилка: Не вдалося отримати вхідний текст.")