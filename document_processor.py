# import pdfplumber
# import pandas as pd
# import os
# from langchain_community.embeddings import HuggingFaceEmbeddings
# from langchain_community.vectorstores import FAISS
# from langchain.text_splitter import RecursiveCharacterTextSplitter

# def deduplicate_columns(headers):
#     """
#     Deduplicate column names by appending a suffix or using generic names if empty.
#     Args:
#         headers: List of column names (may contain duplicates or None).
#     Returns:
#         List of unique column names.
#     """
#     if not headers or all(h is None for h in headers):
#         return [f"Column_{i}" for i in range(len(headers))]
    
#     seen = {}
#     result = []
#     for header in headers:
#         header = str(header) if header is not None else "Unknown"
#         if header in seen:
#             seen[header] += 1
#             result.append(f"{header}_{seen[header]}")
#         else:
#             seen[header] = 0
#             result.append(header)
#     return result

# def process_document(file_path):
#     """
#     Process a document (PDF, Excel, CSV) and extract text and tables.
#     Returns: (text, list of pandas DataFrames)
#     """
#     file_ext = os.path.splitext(file_path)[1].lower()
#     text = ""
#     tables = []
    
#     if file_ext == ".pdf":
#         with pdfplumber.open(file_path) as pdf:
#             for page in pdf.pages:
#                 text += page.extract_text() or ""
#                 page_tables = page.extract_tables()
#                 for table in page_tables:
#                     if table and len(table) > 1:  # Ensure table has headers and data
#                         headers = deduplicate_columns(table[0])
#                         df = pd.DataFrame(table[1:], columns=headers)
#                         tables.append(df)
    
#     elif file_ext == ".xlsx":
#         xls = pd.ExcelFile(file_path)
#         for sheet_name in xls.sheet_names:
#             df = pd.read_excel(file_path, sheet_name=sheet_name)
#             df.columns = deduplicate_columns(df.columns)  # Ensure unique columns
#             tables.append(df)
#             text += df.to_string()  # Convert table to text for RAG
    
#     elif file_ext == ".csv":
#         df = pd.read_csv(file_path)
#         df.columns = deduplicate_columns(df.columns)  # Ensure unique columns
#         tables.append(df)
#         text += df.to_string()  # Convert table to text for RAG
    
#     # Save text embeddings for RAG
#     if text:
#         text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
#         texts = text_splitter.split_text(text)
#         embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
#         vector_store = FAISS.from_texts(texts, embeddings)
#         vector_store.save_local(f"data/{os.path.basename(file_path)}_faiss_index")
    
#     return text, tables





#------------------------------------------------------------------------------------------------------------------------------------

# CODE WITH STORING AND RETRIEVING DATA FROM FAISS
import pdfplumber
import pandas as pd
import os
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.text_splitter import RecursiveCharacterTextSplitter

def deduplicate_columns(headers):
      """
      Deduplicate column names by appending a suffix or using generic names if empty.
      Args:
          headers: List of column names (may contain duplicates or None).
      Returns:
          List of unique column names.
      """
      if not headers or all(h is None for h in headers):
          return [f"Column_{i}" for i in range(len(headers))]
      
      seen = {}
      result = []
      for header in headers:
          header = str(header) if header is not None else "Unknown"
          if header in seen:
              seen[header] += 1
              result.append(f"{header}_{seen[header]}")
          else:
              seen[header] = 0
              result.append(header)
      return result

def process_document(file_path):
      """
      Process a document (PDF, Excel, CSV) and extract text and tables.
      Returns: (text, list of pandas DataFrames)
      """
      file_ext = os.path.splitext(file_path)[1].lower()
      text = ""
      tables = []
      
      if file_ext == ".pdf":
          with pdfplumber.open(file_path) as pdf:
              for page in pdf.pages:
                  extracted_text = page.extract_text() or ""
                  text += extracted_text
                  page_tables = page.extract_tables()
                  for table in page_tables:
                      if table and len(table) > 1:  # Ensure table has headers and data
                          headers = deduplicate_columns(table[0])
                          df = pd.DataFrame(table[1:], columns=headers)
                          tables.append(df)
      
      elif file_ext == ".xlsx":
          xls = pd.ExcelFile(file_path)
          for sheet_name in xls.sheet_names:
              df = pd.read_excel(file_path, sheet_name=sheet_name)
              df.columns = deduplicate_columns(df.columns)  # Ensure unique columns
              tables.append(df)
              text += df.to_string()  # Convert table to text for RAG
      
      elif file_ext == ".csv":
          df = pd.read_csv(file_path)
          df.columns = deduplicate_columns(df.columns)  # Ensure unique columns
          tables.append(df)
          text += df.to_string()  # Convert table to text for RAG
      
      # Save text to file
      if text:
          txt_path = os.path.join("data", f"{os.path.basename(file_path)}_text.txt")
          with open(txt_path, "w", encoding="utf-8") as f:
              f.write(text)
          text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
          texts = text_splitter.split_text(text)
          embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
          vector_store = FAISS.from_texts(texts, embeddings)
          vector_store.save_local(f"data/{os.path.basename(file_path)}_faiss_index")
      
      return text, tables