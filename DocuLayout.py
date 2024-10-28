import os
from dotenv import load_dotenv
from tabulate import tabulate
import io
import copy
import re
from PyPDF2 import PdfReader
# Azure imports
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient 
from azure.ai.documentintelligence.models import AnalyzeResult
from azure.storage.blob import BlobServiceClient

from langchain import hub
from langchain_core.documents import Document
from langchain_openai import AzureChatOpenAI
from langchain_community.document_loaders import AzureAIDocumentIntelligenceLoader
from langchain_openai import AzureOpenAIEmbeddings
from langchain.schema import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough
from langchain.text_splitter import MarkdownHeaderTextSplitter
from langchain.vectorstores.azuresearch import AzureSearch
from azure.search.documents import SearchClient, IndexDocumentsBatch
from azure.search.documents.indexes.models import(
    SimpleField,
    ComplexField,
    SearchableField,
    SearchField,
    SearchFieldDataType,
)







class DocuLayout():
    
    def __init__(self, *args, **kwargs):
        
        self.document_intelligence_client = None
        self.blob_service_client = None
        self.search_client = None
        self.index_name

    #document intelligence functions
    def doc_intelligence(document) -> AnalyzeResult:
        pdf_reader = PdfReader(io.BytesIO(document))
        num_pages = pdf_reader._get_num_pages()
        all_results = []

        for start_page in range(1, num_pages, 2):
            end_page = min(start_page + 2, num_pages)
        
            poller = document_intelligence_client.begin_analyze_document(
                "prebuilt-layout", 
                analyze_request=document, 
                content_type="application/pdf", 
                output_content_format="markdown",
                pages = f"{start_page} - {end_page}" if end_page != num_pages else f"{start_page}"
            )
            result: AnalyzeResult = poller.result()
            all_results.append(result)
    
        return all_results

    # Get tables from document processed with document intelligence 
    def get_table(result: AnalyzeResult):
        tables = []
        table_formated = []
        if result.tables:
            for table in result.tables:
                table_data = []
                headers = []
                
                for cell in table.cells:
                    if cell.row_index == 0:
                        headers.append(cell.content)
                    else:
                        if len(table_data) < cell.row_index:
                            table_data.append([])
                        table_data[cell.row_index-1].append(cell.content)
                
                table_formated.append(tabulate(table_data, headers=headers, tablefmt="simple")) ## store tables for storage
            tables.append(table_formated)
        return tables

    # Format intial dict
    def format_dict(name: str, url: str,):
        
        new_name = name.split(".")[0]
        new_name = new_name.replace(" ", "_")
        result_dict ={
            
            "id": new_name,
            "parent": name,
            "url": url,
        }
        return result_dict

    # pages and document dict
    def pages_dict(first_dict: dict, result: AnalyzeResult, index: int):
        
        page_index = index + 1
        page_index = page_index*2-1 # we are always passing two pages so to get first is 
        doc_pages = []         # index * 2 - 1. Then increment by one for second page. 
        
        for page in result.pages:
            
            lines=[]
            for line in page.lines:
                lines.append(line.content)

            page_dict = {
                'id': f'{first_dict["id"]}_{page_index}',
                'parent': first_dict["parent"],
                'pageNumber': page.page_number,
                'url': first_dict["url"],
                'content': ' '.join(lines)
            }
            doc_pages.append(page_dict)
            
            page_index += 1

            
        return doc_pages
    # create documents to send for AI search and tables to merge. 
    def prep_to_send(doc_pages: list):
        
        docs_to_send = []
        
        for page in doc_pages:
            metadata = {
                "id": page['id'],
                "parent": page["parent"],
                "pageNumber": page['pageNumber'],
                "url": page['url'],
            }
            doc = Document(page_content=page["content"])
            doc.metadata = metadata
            docs_to_send.append(doc)
            
        return docs_to_send