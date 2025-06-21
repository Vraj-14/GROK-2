import streamlit as st
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import asyncio
import os
from document_processor import process_document
from table_query import query_table
from rag_pipeline import query_document
import threading
import sqlite3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Ensure upload and data directories exist
os.makedirs("uploads", exist_ok=True)
os.makedirs("data", exist_ok=True)

# SQLite database for table storage
def init_db():
    conn = sqlite3.connect("data/tables.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS tables 
                 (id INTEGER PRIMARY KEY, file_name TEXT, table_index INTEGER, table_data TEXT)""")
    conn.commit()
    conn.close()

init_db()

# FastAPI endpoint for file upload
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_path = os.path.join("uploads", file.filename)
        with open(file_path, "wb") as f:
            f.write(await file.read())
        
        # Process document (extract text and tables)
        text, tables = process_document(file_path)
        
        # Store tables in SQLite
        conn = sqlite3.connect("data/tables.db")
        c = conn.cursor()
        for idx, table in enumerate(tables):
            try:
                table_json = table.to_json(orient="columns")
                c.execute("INSERT INTO tables (file_name, table_index, table_data) VALUES (?, ?, ?)",
                         (file.filename, idx, table_json))
            except Exception as e:
                logger.error(f"Error storing table {idx} for {file.filename}: {e}")
        conn.commit()
        conn.close()
        
        return JSONResponse({"message": "File processed successfully", "filename": file.filename})
    except Exception as e:
        logger.error(f"Error processing file {file.filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# FastAPI endpoint for querying
@app.post("/query")
async def query(request: dict):
    try:
        question = request.get("question")
        filename = request.get("filename")
        if not question or not filename:
            raise HTTPException(status_code=400, detail="Question and filename required")
        
        # Check if question is table-related (simple heuristic)
        if any(keyword in question.lower() for keyword in ["table", "row", "column", "sum", "average","user ID","User ID","ID","Gender","Age","Country","Purchased","Salary","CIN", "CIN Number", "CIN number","No. of Shares as on 31.03.2023","No. of Shares as on 31.03.2024","Salary as on 31, March, 2023","Salary as on 31, March, 2024","Balance Outstanding as on 31.03.2024","Relationship with the struck off company"]):
            # Query tables using TAPAS
            conn = sqlite3.connect("data/tables.db")
            c = conn.cursor()
            c.execute("SELECT table_data FROM tables WHERE file_name = ?", (filename,))
            tables = c.fetchall()
            conn.close()
            
            if not tables:
                return JSONResponse({"answer": "No tables found for this file"})
            
            # Query each table
            for table_data in tables:
                answer = query_table(table_data[0], question)
                if answer:
                    return JSONResponse({"answer": answer})
            return JSONResponse({"answer": "No relevant answer found in tables"})
        else:
            # Query document text using RAG
            answer = query_document(filename, question)
            return JSONResponse({"answer": answer})
    except Exception as e:
        logger.error(f"Error querying: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Streamlit UI
def run_streamlit():
    st.title("Document Chatbot")
    st.write("Upload a PDF, Excel, or CSV file and ask questions about its content.")

    # File upload
    uploaded_file = st.file_uploader("Choose a file", type=["pdf", "xlsx", "csv"])
    if uploaded_file:
        file_path = os.path.join("uploads", uploaded_file.name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        # Call FastAPI upload endpoint
        import requests
        files = {"file": (uploaded_file.name, open(file_path, "rb"))}
        response = requests.post("http://localhost:8000/upload", files=files)
        if response.status_code == 200:
            st.success("File processed successfully!")
            st.session_state["filename"] = uploaded_file.name
        else:
            st.error(f"Error: {response.json()['detail']}")

    # Chat interface
    if "filename" in st.session_state:
        st.subheader("Ask a Question")
        question = st.text_input("Enter your question")
        if st.button("Submit"):
            if question:
                # Call FastAPI query endpoint
                response = requests.post("http://localhost:8000/query", 
                                       json={"question": question, "filename": st.session_state["filename"]})
                if response.status_code == 200:
                    st.write(f"**Answer**: {response.json()['answer']}")
                else:
                    st.error(f"Error: {response.json()['detail']}")
            else:
                st.warning("Please enter a question.")

# Run FastAPI server in a separate thread
def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    # Start FastAPI in a thread
    threading.Thread(target=run_fastapi, daemon=True).start()
    # Run Streamlit
    run_streamlit()