import fitz
import subprocess
import time
from datetime import datetime
import glob
import re
import numpy as np
from numpy.core.arrayprint import format_float_scientific
import pandas as pd
from os import walk
import os
import glob
from pathlib import Path
from openpyxl import load_workbook
from shutil import copyfile
import requests
import pymysql
import pymysql.cursors
from json import loads
import csv
import json
from sqlalchemy import create_engine
from urllib.parse import quote


def sort_blocks(blocks):
    """
    Sort the blocks of a TextPage in ascending vertical pixel order,
    then in ascending horizontal pixel order.
    This should sequence the text in a more readable form, at least by
    convention of the Western hemisphere: from top-left to bottom-right.
    If you need something else, change the sortkey variable accordingly ...
    """

    sblocks = []
    for b in blocks:
        x0 = str(int(b["bbox"][0] + 0.99999)).rjust(4, "0")  # x coord in pixels
        y0 = str(int(b["bbox"][1] + 0.99999)).rjust(4, "0")  # y coord in pixels
        sortkey = y0 + x0  # = "yx"
        sblocks.append([sortkey, b])
    sblocks.sort()
    return [b[1] for b in sblocks]  # return sorted list of blocks

def sort_spans(spans):
    """Sort the spans of a line in ascending horizontal direction. See comment
    in sort_blocks function.
    """
    sspans = []
    for s in spans:
        x0 = str(int(s["bbox"][0] + 0.99999)).rjust(4, "0")
        sspans.append([x0, s])
    sspans.sort()
    return [s[1] for s in sspans]

def get_tessocr(page, bbox):
    """Return OCR-ed span text using Tesseract.
    Args:
        page: fitz.Page
        bbox: fitz.Rect or its tuple
    Returns:
        The OCR-ed text of the bbox.
    """
    global ocr_time, pix_time, tess, mat
    # Step 1: Make a high-resolution image of the bbox.
    
    t0 = time.perf_counter()
    pix = page.get_pixmap(
        colorspace  =fitz.csGRAY,  # we need no color
        matrix      = mat,
        clip        = bbox,
    )
    image = pix.tobytes("png")  # make a PNG image
    
    t1 = time.perf_counter()
    # Step 2: Invoke Tesseract to OCR the image. Text is stored in stdout.
    rc = subprocess.run(
        tess,  # the command
        input   = image,  # the pixmap image
        stdout  = subprocess.PIPE,  # find the text here
        shell   = True,
    )
    # because we told Tesseract to interpret the image as one line, we now need
    # to strip off the line break characters from the tail.
    text = rc.stdout.decode()  # convert to string
    text = re.sub(r"\n\x0c", "", text)  # remove line end characters
    
    t2 = time.perf_counter()
    ocr_time += t2 - t1
    pix_time += t1 - t0
    return text


def parse_pdf(path):
    results = []
    try:
        doc = fitz.open(path)
        ocr_count = 0
        for page in doc:
            page_get_text = page.get_text("dict", flags=0)
            blocks = sort_blocks(page_get_text["blocks"])
            for b in blocks:
                # lines = SortLines(b["lines"])            # ... lines
                for l in b["lines"]:
                    spans = sort_spans(l["spans"])  # ... spans
                    for s in spans:
                        text = s["text"]
                        if chr(65533) in text:  # invalid characters encountered!
                            # invoke OCR
                            ocr_count += 1
                            # print("before: '%s'" % text)
                            text1 = text.lstrip()
                            sb = " " * (len(text) - len(text1))  # leading spaces
                            text1 = text.rstrip()
                            sa = " " * (len(text) - len(text1))  # trailing spaces
                            text = sb + get_tessocr(page, s["bbox"]) + sa
                            text = text.strip()
                            # print(" after: '%s'" % text)
                        # else:
                        # print("non ocr: '%s'" % text)
                        # if "6." in text and ":" in text:
                        #     return results
                        # else:
                        results.append(text)
                        
        x = ' '.join(results)
        # print("-------------------------")
        # print("OCR invocations: %i." % ocr_count)
        # print(
        #     "Pixmap time: %g (avg %g) seconds."
        #     % (round(pix_time, 5), round(pix_time / ocr_count, 5))
        # )
        # print(
        #     "OCR time: %g (avg %g) seconds."
        #     % (round(ocr_time, 5), round(ocr_time / ocr_count, 5))
        # )
    except Exception as e:
        print(e)
    return x

if __name__ == '__main__':
    mat = fitz.Matrix(4, 4)
    tess = "tesseract stdin stdout --oem 3 --psm 6 -l vie"
    ocr_time = 0
    pix_time = 0
    path = '/Users/dinhvan/Document/Projects/ocr/01_06_2022_1_0.pdf'
    print(parse_pdf(path))