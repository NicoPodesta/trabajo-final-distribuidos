FROM python:3.12-slim

RUN pip install --no-cache-dir \
    numpy==2.5.4 pandas==3.1.0 pyarrow==26.0.0 nltk==3.10.4 \
    jupyterlab==4.6.4 nbconvert==7.17.2 ipykernel==7.4.0

ENV NLTK_DATA=/usr/share/nltk_data
RUN python -m nltk.downloader -d /usr/share/nltk_data stopwords && chmod -R a+rX /usr/share/nltk_data

WORKDIR /repo
