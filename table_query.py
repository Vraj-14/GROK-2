from transformers import TapasTokenizer, TapasForQuestionAnswering
import pandas as pd
import json

# Initialize TAPAS model and tokenizer
model_name = "google/tapas-base-finetuned-wtq"
tokenizer = TapasTokenizer.from_pretrained(model_name)
model = TapasForQuestionAnswering.from_pretrained(model_name)

def query_table(table_json, question):
    """
    Query a table (JSON string) using TAPAS.
    Returns: Answer as a string or None if no answer found.
    """
    try:
        # Convert JSON to DataFrame
        df = pd.read_json(table_json)
        
        # Handle duplicate column names by appending an index
        columns = df.columns
        new_columns = []
        seen = {}
        for col in columns:
            if col in seen:
                seen[col] += 1
                new_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_columns.append(col)
        df.columns = new_columns
        
        # Prepare inputs for TAPAS
        inputs = tokenizer(table=df, queries=[question], padding=True, truncation=True, return_tensors="pt")
        
        # Run inference
        outputs = model(**inputs)
        predicted_answer_coordinates = outputs.logits.argmax(-1).nonzero(as_tuple=False)
        
        # Extract answer
        if predicted_answer_coordinates.numel() > 0:
            answers = []
            for coord in predicted_answer_coordinates:
                row, col = coord[1], coord[2]
                answers.append(str(df.iloc[row, col]))
            return ", ".join(answers)
        return None
    except Exception as e:
        print(f"Error querying table: {e}")
        return None