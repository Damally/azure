# Databricks notebook source
# MAGIC %md ![Databricks for AI Lab](https://www.hbf.com.au/Resources/hbf.com.au/images/hbf-logo-get-well.svg?v=1.0.6.10 "Databricks for Ai Lab") 

# COMMAND ----------

# MAGIC %md # Summarizing Text

# COMMAND ----------

# MAGIC %md In this notebook, you will get to experiment with performing extraction based summarization of text. This technique of summarization attempts to identify key sentences in a provided text and returns a summary that is the result of returning just those key sentences. 

# COMMAND ----------

# MAGIC %md The process we will follow to summarize text is a subset of the text analytics pipeline that includes these steps:  
# MAGIC - Normalize the text: in this case, simply to clean the text of line break characters. This followed by some simple sentence tokenization, which breaks the paragraph into sentences and removes any extra trailing spaces. Finally the cleaned up text is returned as string.
# MAGIC - Apply the analytic method: in this case, we use the summarize method provided by the gensim library to generate the summarized result.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Task 1 - Complete NLTK installation

# COMMAND ----------

# MAGIC %md
# MAGIC NLTK is a rich toolkit with modular components, many of which are not installed by default. To install all the components, run the following cell.

# COMMAND ----------

import nltk
nltk.download('all')

# COMMAND ----------

# MAGIC %md ## Task 2 - Import modules

# COMMAND ----------

# MAGIC %md First, we need to import the modules used by our logic.

# COMMAND ----------

import re
import unicodedata
import numpy as np
from gensim.summarization import summarize

# COMMAND ----------

# MAGIC %md NOTE: You can safely ignore the warning in the above cell that indicates a warning "aliasing chunksize to chunksize_serial".

# COMMAND ----------

# MAGIC %md ## Task 3 - Normalize text

# COMMAND ----------

# MAGIC %md In the following, we define a method that will remove line break characters, tokenize the paragraph of text into an array of string sentences and then strip any extra spaces surrounding a sentence. This is an example of a simple, but typical, text normalization step.

# COMMAND ----------

def clean_and_parse_document(document):
    document = re.sub('\n', ' ', document)
    document = document.strip()
    sentences = nltk.sent_tokenize(document)
    sentences = [sentence.strip() for sentence in sentences]
    return sentences

# COMMAND ----------

# MAGIC %md ## Task 4 - Summarize text

# COMMAND ----------

# MAGIC %md In the following, we define a method that uses the summarize method from the gensim module. We take the pre-processed output from our clean_and_parse_document routine and convert the array of string sentences to a single text item by concatenating the sentences. When performing text analytics, some analytic methods might require tokenized input and others may require string input, so this is a common process. In this, the summarize method requires a text string as input.

# COMMAND ----------

def summarize_text(text, summary_ratio=None, word_count=30):
    sentences = clean_and_parse_document(text)
    cleaned_text = ' '.join(sentences)
    summary = summarize(cleaned_text, split=True, ratio=summary_ratio, word_count=word_count)
    return summary 

# COMMAND ----------

# MAGIC %md ## Task 5 - Try it out

# COMMAND ----------

# MAGIC %md Author an example string that represents a rather long claim description that Contoso Ltd. might encounter. An example is provided for you, but feel free to provide your own. 

# COMMAND ----------

example_document = """
I was driving down El Camino and stopped at a red light.
It was about 3pm in the afternoon.  
The sun was bright and shining just behind the stoplight.
This made it hard to see the lights.
There was a car on my left in the left turn lane.
A few moments later another car, a black sedan pulled up behind me. 
When the left turn light changed green, the black sedan hit me thinking 
that the light had changed for us, but I had not moved because the light 
was still red.
After hitting my car, the black sedan backed up and then sped past me.
I did manage to catch its license plate. 
The license plate of the black sedan was ABC123. 
"""

# COMMAND ----------

# MAGIC %md Now, invoke your summarize_text function against the example document and observe the result.

# COMMAND ----------

summarize_text(example_document)

# COMMAND ----------

# MAGIC %md Observe that the summary is returned as an array of string. If multiple sentences were returned, there would be multiple array entries.

# COMMAND ----------

# MAGIC %md ## Task 6 - Experiment

# COMMAND ----------

# MAGIC %md 1. The summarize text function above defaults to providing a summary that is about 30 words long. What happens if you attempt to summarize the text to 60 words?
# MAGIC 2. What happens when you submit a text to summarize that is shorter than the summary target length?