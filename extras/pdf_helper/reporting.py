#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  5 20:36:56 2016

@author: john funk
"""

# PlatypusTable Test
import pandas as pd
import numpy as np
import io
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.rl_config import defaultPageSize
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import PageBreak
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
import datetime
from reportlab.lib.utils import ImageReader
DEFAULT_FOOTER = "Page: @@PAGE     Generated: @@TIMESTAMP     From file: @@SOURCEFILE"

def default_first_page_layout(canvas, doc):
    report = doc.__report__
    canvas.saveState()
    canvas.setFont('Times-Bold', 16)
    canvas.drawCentredString(report.PAGE_WIDTH / 2.0, 8 * inch, report.title)
    canvas.setFont('Times-Roman', 9)
    template = report.first_page_footer_str
    template = template.replace("@@PAGE", str(doc.page))
    template = template.replace("@@TIMESTAMP", str(doc.page))
    template = template.replace("@@SOURCEFILE", str(doc.page))
    canvas.drawString(inch, 0.75 * inch, template)
    canvas.restoreState()

# define layout for subsequent pages
def default_later_page_layout(canvas, doc):
    report = doc.__report__
    canvas.saveState()
    canvas.setFont('Times-Roman', 9)
    #canvas.drawString(inch, 0.75 * inch, "Page %d %s" % (doc.page, PDFReport.pageinfo))
    canvas.restoreState()

# define layout for subsequent pages
def default_page_layout(canvas, doc, footer_str=DEFAULT_FOOTER, logo_data: io.BytesIO = None):
    report = doc.__report__
    footer_str = footer_str.replace("@@PAGE", str(doc.page))
    footer_str = footer_str.replace("@@TIMESTAMP", str(doc.page))
    footer_str = footer_str.replace("@@SOURCEFILE", str(report.sourcefile))
    canvas.saveState()
    if logo_data:
        logo = ImageReader(logo_data)
        canvas.drawImage(logo, .5 * inch, 6.7 * inch, mask='auto')
    canvas.setFont('Times-Roman', 9)
    canvas.drawString(inch * 3, 0.75 * inch, footer_str)
    canvas.restoreState()

class PDFReport(object):
    def __init__(self,sourcefile:str=None):
        self.doc = SimpleDocTemplate("/tmp/document.pdf", pagesize=landscape(letter))
        self.title = "no title"
        self.docElements = []
        #setup the package scoped global variables we need
        now = datetime.datetime.now()
        self.timestamp = now.strftime("%Y-%m-%d %H:%M")
        self.first_page_footer_str = "Page: @@PAGE     Generated: @@TIMESTAMP     From file: @@SOURCEFILE"
        self.sourcefile = sourcefile if sourcefile else "not initialized"
        self.pageinfo =  "not initialized"
        self.PAGE_HEIGHT = defaultPageSize[1]
        self.PAGE_WIDTH = defaultPageSize[0]
        self.styles = getSampleStyleSheet()   #sample style sheet doesn't seem to be used
        setattr(self.doc,"__sourcefile__",self.sourcefile)
        setattr(self.doc,"__report__",self)

    def add_table(self,df:pd.DataFrame):
        data = [df.columns[:, ].values.astype(str).tolist()] + df.values.tolist()
        elements = []
         # Data Frame
        t = Table(data)
        t.setStyle(TableStyle([('FONTNAME', (0, 0), (-1, -1), "Helvetica"),
                               ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                               ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
                               ('BOX', (0, 0), (-1, -1), 0.25, colors.black)]))
        elements.append(t)
        elements.append(Spacer(1, 0.2 * inch))
        self.docElements.extend(elements)
    def add_paragraph(self, text, style="Normal"):
        paragraph = Paragraph(text, self.styles['Normal'])
        self.docElements.append(paragraph)

    def pagebreak(self):
        self.docElements.extend([PageBreak()])

    def write_pdfpage(self, first_layout=default_first_page_layout,  later_layout=default_later_page_layout):
        self.doc.build(self.docElements, onFirstPage=first_layout, onLaterPages=later_layout)
