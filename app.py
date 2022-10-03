#!/usr/bin/env python
# coding: utf-8

import os
import math
import hashlib
import statistics
from logging import getLogger
from datetime import datetime
import matplotlib.pyplot as plt
import japanize_matplotlib
import pandas as pd
import streamlit as st
# try:
#     from streamlit.legacy_caching import clear_cache
# except ImportError as e:
#     logger.info("Import error '%s'", e)
#     from streamlit.caching import clear_cache
    
from google.cloud import storage
logger = getLogger(__file__)

LOCAL_CSVFILE = "data.csv.gz"

def _get_md5(filename, blocksize=2**20):
    # compute md5 checksum of a file
    md5 = hashlib.md5()
    with open(filename, "rb") as f:
        while True:
            data = f.read(blocksize)
            if len(data) == 0:
                break
            md5.update(data)
    return md5.hexdigest()


def update_datafile():
    params = "gcp_service_account", "gcp_bucket", "csvfilename", "md5filename"
    for p in params:
        assert p in st.secrets, f"'{p}' is missing"

    client = storage.Client.from_service_account_info(st.secrets["gcp_service_account"])
    bucket = client.bucket(st.secrets["gcp_bucket"])


    if os.path.isfile(LOCAL_CSVFILE):
        # compare the md5 hash of the current file against the value in the remote
        # download the file only if they are different
        local_md5 = _get_md5(LOCAL_CSVFILE)
        md5 = bucket.blob(st.secrets["md5filename"]).download_as_string().decode("utf8")
        logger.info("'%s' vs '%s'", local_md5, md5)
        if local_md5 == md5:
            logger.info("md5 hash is the same, so no need to download the file")
            return
        logger.info("md5 hash is the different, so we will download the new file")

    bucket.blob(st.secrets["csvfilename"]).download_to_filename(LOCAL_CSVFILE)
    logger.info("Downloaded file to '%s'", LOCAL_CSVFILE)
    #updated = bucket.get_blob(st.secrets["csvfilename"]).updated
    #logger.info("Data file updated at: %s", updated)


    # try:
    #     st.legacy_caching.clear_cache()
    # except Exception as e:
    #     logger.info("Error with 'legacy_caching' module: '%s', will try 'caching module'", e)
    #     st.caching.clear_cache()
    load_data.clear()
    logger.info("Cache cleared")


@st.experimental_singleton
def load_data()-> pd.DataFrame:
    logger.info("Reading data from '%s'", LOCAL_CSVFILE)
    x = pd.read_csv(LOCAL_CSVFILE)
    return x


def make_histograms(df: pd.DataFrame,
                    maxq: int=None, trial: int=None, username: str="",
                    show_percent: bool=False, include_maxscore: bool=False,
                    nrow: int=3, ncol: int=3):
    logger.info("Making histograms")
    df = df.copy()
    df = df[~df.hasreference]
    df = df.dropna(subset=["score", "scoremax", "qnumber", "trycount"])
    df.qnumber = df.qnumber.astype(int)
    df.score = df.score.astype(int)
    df.scoremax = df.scoremax.astype(int)
    if show_percent:
        df.score = (100.0 * df.score / df.scoremax)
        df.scoremax = 100

    if trial is not None:
        logger.info("Filtering to trial = %s", trial)
        df = df[df.trycount == trial]
        logger.info("Data shape: %s", df.shape)
    if maxq is not None:
        logger.info("Filtering to question '%s' or before", maxq)
        df = df[df.qnumber <= maxq]
        logger.info("Data shape: %s", df.shape)
    if not include_maxscore:
        logger.info("Filtering out scores greater than or equal to the max score")
        df = df[df.score < df.scoremax]
        logger.info("Data shape: %s", df.shape)    
    logger.info("Remaining data shape: %s", df.shape)

    qnums = list(set(df.qnumber))
    qnums.sort(reverse=True)
    logger.info("Found %d question numbers: %s", len(qnums), qnums)
    n = min(nrow*ncol, len(qnums))
    logger.info("Will plot %d histograms", n)
    nrow = math.ceil(n / ncol)
    logger.info("nrow, ncol: %d, %d", nrow, ncol)
    fig = plt.Figure(figsize=(ncol*4.5, nrow*3))
    for i in range(n):
        a = fig.add_subplot(nrow, ncol, i+1)

        q = qnums[i]
        tmp = df[df.qnumber == q]
        maxscore = statistics.mode(tmp.scoremax)
        logger.info("Max score for question '%s': %s", q, maxscore)
        #bars = tmp.groupby("score", as_index=False).size()
        #print(bars.score, bars.size)
        #a.bar(bars.score, bars.size)
        a.grid(linestyle="dotted")
        a.hist(tmp.score, bins=30, edgecolor="#dddddd")
        a.set_title(f"{q} (満点:{maxscore}) | 平均={tmp.score.mean():.1f}, 中央値={tmp.score.median():.1f}")

        if username != "":
            tmp2 = tmp[tmp.username==username]
            if len(tmp2) == 0:
                logger.info("User '%s' not found in qnumber %s", username, q)
            else:
                if len(tmp2) > 1:
                    logger.warning("Multiple rows  of the same user '%s':\n%s", username, tmp2)
                    tmp2 = tmp2.sort_values("score").head(0)  # we take the smallest score

                userscore = tmp2.score.item()
                userposition = (tmp.score >= userscore).mean()
                logger.info("Score of user '%s': %s, top %s", username, userscore, userposition)
                a.axvline(userscore, color="orange", linestyle="dashed", linewidth=2)
                a.text(0.05, 0.9, f"{username}\n{int(userscore)}, top {round(100*userposition)}%",
                       horizontalalignment="left", verticalalignment="top", transform=a.transAxes,
                       bbox={"facecolor":"orange", "alpha":0.5, "boxstyle":"round"})
                       #fontdict={"alpha":0.5, "backgroundcolor":"orange"})

    fig.tight_layout()
    return fig


def main():
    logger.info("Start the app")
    st.set_page_config(page_title="クイズMondo 得点分布", layout="wide")

    logger.info("Fetch new file if any")
    update_datafile()

    x = load_data()
    logger.info("Data shape: %s", x.shape)
    logger.info("Columns:\n%s", x.columns)
    logger.info("Data head:\n%s", x.head())
    logger.info("Data summary:\n%s", x.describe())

    st.markdown("""
    #### [Mondo (指定オープンクイズ)](https://mondo.quizknock.com/) の得点分布
    ※ Twitterへ投稿された結果の集計
    """)

    cols = st.columns(6)
    with cols[0]:
        username = st.text_input("ユーザー名", placeholder="Twitter ユーザー名")
    with cols[1]:
        # find the most frequent date
        tmp = x[["qnumber", "datetime"]].dropna().copy()
        tmp.qnumber = tmp.qnumber.astype(int)
        tmp["date"] = pd.to_datetime(x.datetime).dt.tz_convert("Asia/Tokyo").dt.strftime("%Y/%m/%d")
        tmp = tmp.groupby("qnumber").date.agg(statistics.mode).sort_index(ascending=False)
        logger.info("Question number and the most frequent date:\n%s", tmp)
        maxq = st.selectbox("問題番号", tmp.index, format_func=lambda q: f"{q} ({tmp[q]})")
    with cols[2]:
        trial = st.selectbox("回答回数", [None, 1, 2, 3], format_func=lambda t: "すべて" if t == None else f"{t}回目")        
    with cols[3]:
        show_percent = st.checkbox("パーセントスコア")
        include_maxscore = st.checkbox("満点を含む")
    with cols[4]:
        nrow = st.selectbox("表示行数", [1,2,3,4,5,6], index=1)
    with cols[5]:
        ncol = st.selectbox("表示列数", [1,2,3,4,5,6], index=2)

    max_datetime = pd.to_datetime(x.datetime.max()).tz_convert("Asia/Tokyo").strftime("%Y/%m/%d %H:%M:%S")
    cols = st.columns(5)
    with cols[0]:
        st.text(f"Data as of: {max_datetime}")
    with cols[1]:
        obj = x[~x.hasreference].dropna(subset=["score", "scoremax", "qnumber", "trycount"]).to_csv(index=False).encode("utf8")
        st.download_button("Download data", obj, file_name="mondoquiz-app.csv", mime="text/csv")

    fig = make_histograms(x, maxq=maxq, trial=trial, nrow=nrow, ncol=ncol, username=username,
                          include_maxscore=include_maxscore, show_percent=show_percent)
    st.pyplot(fig)
    st.markdown("---")
    st.markdown("Copyright&nbsp;&copy;Kota Mori &nbsp; | &nbsp; View the source at [GitHub](https://github.com/kota7/mondoquiz-app).",
                unsafe_allow_html=True)

if __name__ == "__main__":
    main()