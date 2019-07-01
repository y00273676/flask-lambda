# /usr/bin/env python
# coding=utf8

from io import BytesIO

import boto3
from flask import Flask, jsonify, request
from sqlalchemy import create_engine

S3_BUCKET = "my-aws-two"
S3_KEY_PREFIX = "test/{}"
MYSQL_USER_NAME = "zhimaaread"
MYSQL_PASSWORD = "zhimaa"
MYSQL_HOST = "fanslave4.c6cm6m48rq3l.rds.cn-north-1.amazonaws.com.cn"
MYSQL_DATABASE = "fan"


def get_connection():
    engine = create_engine(f"mysql://{MYSQL_USER_NAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}",
                           connect_args={"encoding": "utf8"})
    return engine.connect()


def result_json(code, desc, data):
    return {
        "resultCode": code,
        "resultDescription": desc,
        "data": data
    }


app = Flask(__name__)


@app.route("/to-production", methods=["POST"])
def to_production():
    content = request.json
    file_name = content.get("fileName", "")
    if file_name == "":
        return jsonify(result_json("ParamError", "fileName error", ""))
    f = BytesIO()
    s3 = boto3.client('s3')
    s3.download_fileobj(S3_BUCKET, S3_KEY_PREFIX.format(file_name), f)
    res = f.getvalue().decode('utf8').strip()
    return jsonify(res)
