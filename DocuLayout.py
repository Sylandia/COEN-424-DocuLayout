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
        
        # Load environment variables
        load_dotenv()
        os.environ["AZURE_OPENAI_ENDPOINT"] = os.getenv("OPENAI_ENDPOINT")
        os.environ["AZURE_OPENAI_API_KEY"] = os.getenv("OPENAI_KEY")
        doc_intelligence_endpoint = os.getenv("DOC_INTELLIGENCE_ENDPOINT")
        doc_intelligence_key = os.getenv("DOC_INTELLIGENCE_KEY")
        blob_connection_string = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
        storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")
        search_endpoint: str = os.getenv("AI_SEARCH_ENDPOINT")
        search_admin_key: str = os.getenv("AI_SEARCH_KEY")
        index_name: str = os.getenv("INDEX_NAME").lower() #keep lowercase and in the format of "container_name-index"
        container_name = index_name.split("-index")[0]
      

        # Connect to search service 
        
        self.document_intelligence_client = DocumentIntelligenceClient(endpoint= doc_intelligence_endpoint, credential=AzureKeyCredential(doc_intelligence_key))
        self.blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        self.search_client = SearchClient(endpoint = search_endpoint, index_name = index_name, credential = AzureKeyCredential(search_admin_key))
        blob_client = self.blob_service_client.get_container_client(f"container_name")
        if not blob_client.exists():
            blob_client.create_container()
            print("Created")
        self.list_blobs = blob_client
        self.index_name = index_name

    def dot_env_load(self):
        
        load_dotenv()
        os.environ["AZURE_OPENAI_ENDPOINT"] = os.getenv("OPENAI_ENDPOINT")
        os.environ["AZURE_OPENAI_API_KEY"] = os.getenv("OPENAI_KEY")
        self.doc_intelligence_endpoint = os.getenv("DOC_INTELLIGENCE_ENDPOINT")
        self.doc_intelligence_key = os.getenv("DOC_INTELLIGENCE_KEY")
        self.blob_connection_string = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
        self.storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")
        self.search_endpoint: str = os.getenv("AI_SEARCH_ENDPOINT")
        self.search_admin_key: str = os.getenv("AI_SEARCH_KEY")
        self.index_name: str = os.getenv("INDEX_NAME")


    #document intelligence functions
    def doc_intelligence(self,document) -> AnalyzeResult:
        pdf_reader = PdfReader(io.BytesIO(document))
        num_pages = pdf_reader._get_num_pages()
        all_results = []

        for start_page in range(1, num_pages, 2):
            end_page = min(start_page + 2, num_pages)
        
            poller = self.document_intelligence_client.begin_analyze_document(
                "prebuilt-layout", 
                analyze_request=document, 
                content_type="application/pdf", 
                output_content_format="markdown",
                pages = f"{start_page} - {end_page}" if end_page != num_pages else f"{start_page}"
            )
            result: AnalyzeResult = poller.result()
            all_results.append(result)
    
        return all_results

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