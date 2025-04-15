from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
import torch
import pandas as pd

quantization_config = BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_use_double_quant=True)

model_name = "aaditya/OpenBioLLM-Llama3-8B"

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    quantization_config=quantization_config,
    device_map="auto"
)

def get_ner_summary(text):
    prompt = f"Extraia entidades NER (PESSOA, LOCAL, DATA, etc.) estruturadamente:\n\n{text}\n\nOutput JSON:"
    inputs = tokenizer(prompt, return_tensors="pt").to("cuda")
    output = model.generate(**inputs, max_new_tokens=200)
    result = tokenizer.decode(output[0], skip_special_tokens=True)
    return parse_json_output(result)

df = pd.read_parquet('sources/full_df.parquet')

# Aplica função ao dataframe original, criando novas colunas automaticamente
df['ner_result'] = df['texto_original'].apply(get_ner_summary)

# Expandir resultados em novas colunas
df_expanded = pd.json_normalize(df['ner_result'])
df_final = pd.concat([df, df_expanded], axis=1)

df_final.to_csv('resultado_final.csv', index=False)
