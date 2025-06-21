from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_community.llms import Ollama
from langchain.chains import RetrievalQA
import os

def query_document(filename, question):
    """
    Query a document's text using Ollama-based RAG with Phi-3.
    Returns: Answer as a string.
    """
    try:
        # Load FAISS index
        embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
        vector_store = FAISS.load_local(f"data/{filename}_faiss_index", embeddings, allow_dangerous_deserialization=True)
        
        # Initialize Ollama LLM with Phi-3
        llm = Ollama(model="phi3")
        
        # Set up RAG chain
        qa_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=vector_store.as_retriever(),
            return_source_documents=False
        )
        
        # Run query
        result = qa_chain({"query": question})
        return result["result"]
    except Exception as e:
        return f"Error querying document: {e}"