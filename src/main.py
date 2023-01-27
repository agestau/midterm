
import json
from json import dumps, loads
import numpy as np
import pandas as pd
from typing import Dict, List
import datetime

from fastapi import FastAPI
from numpy.typing import NDArray

from src.models import book
from src.utils.model_utils import load_model

from src.db.connection import db_connection

import cloudpickle

# loading model
# book_model = load_model("https://www.dropbox.com/s/wmitlsugemqvcb3/book_model.pkl?dl=0")
book_model = cloudpickle.load(open("D:/CA_AI_2022/midterm/book_model.pkl", "rb"))

app = FastAPI()
cursor = db_connection.cursor()

@app.post("/create_books")
def create_books():
    sql = """CREATE TABLE IF NOT EXISTS books (
            id int NOT NULL AUTO_INCREMENT,
            title varchar(255),
            about varchar(5000),
            genre varchar(255),
            PRIMARY KEY(id)
        );"""
    cursor.execute(sql)
    return {"'books' created"}


@app.post("/create_requests")
def create_requests():
    sql = """CREATE TABLE IF NOT EXISTS requests (
            id int NOT NULL AUTO_INCREMENT,
            path varchar(255) NOT NULL,
            input varchar(20000) NOT NULL,
            prediction varchar(255) NOT NULL,
            time timestamp NOT NULL,
            PRIMARY KEY(id)
        );"""
    cursor.execute(sql)
    return {"'requests' created"}


def insert_df_into_table(dataf):
    cursor = db_connection.cursor()
    lst = dataf.values.tolist()
    sql = 'INSERT INTO books (title, about, genre) VALUES (%s, %s, %s)'
    cursor.executemany(sql, lst)
    db_connection.commit()
    print("DF inserted")
    cursor.close()


@app.get("/health")
def health():
    return {"healthy!"}


@app.get("/book")
def get_books():
    cursor = db_connection.cursor()
    sql = "SELECT * from books"
    cursor.execute(sql)
    return [val for val in cursor]


@app.post("/predict_book")
def predict_books(inputs: List[book.Input]) -> Dict[str, List[Dict[str, float]]]:
    parsed_input = pd.DataFrame([i.dict() for i in inputs])
    outputs: NDArray[np.float32] = book_model.predict(parsed_input["text"].values)
    return {"outputs": [{"class": i} for i in outputs.tolist()]}


@app.post("/predict_save_req")
def predict_save_req(inputs: List[book.Input]) -> Dict[str, List[Dict[str, float]]]:
    parsed_input = pd.DataFrame([i.dict() for i in inputs])
    outputs: NDArray[np.float32] = book_model.predict(parsed_input["text"].values)
    
    inputs_str=', '.join(str(x) for x in inputs)

    outputs_lst=outputs.tolist()
    outputs_str=', '.join(str(x) for x in outputs_lst)

    sql = "INSERT INTO requests (path, input, prediction, time) VALUES (%s, %s, %s, %s)"
    path = "/predict"
    input = inputs_str
    prediction = outputs_str
    time = datetime.datetime.now()
    cursor.execute(sql, (path, input, prediction, time))
    db_connection.commit()

    return {"predictions": [{"genre": i} for i in outputs.tolist()]}


@app.get("/get_requests")
def get_requests():
    sql = "SELECT * FROM requests ORDER BY time DESC LIMIT 3"
    cursor.execute(sql)
    records = cursor.fetchall()
    db_connection.commit()
    cursor.close()
    return {"requests": records}
