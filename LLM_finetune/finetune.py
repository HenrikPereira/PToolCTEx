# !pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu126
# !pip3 install safetensors pytest tensorboard deepspeed transformers datasets accelerate bitsandbytes trl peft

import os
from transformers import AutoTokenizer, AutoModelForCausalLM, Trainer, TrainingArguments, BitsAndBytesConfig
from datasets import load_dataset
import torch

model_name = "aaditya/OpenBioLLM-Llama3-8B"

# Certifique-se de que a pasta de offload existe
os.makedirs("./offload", exist_ok=True)

# Carregar tokenizador
print("Exemplo de teste")
tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=False)
print(tokenizer("Exemplo de teste"))

# Configuração para quantização 8-bit e offload para CPU
quantization_config = BitsAndBytesConfig(
    load_in_4bit=True, bnb_4bit_use_double_quant=True, bnb_4bit_quant_type="nf4", bnb_4bit_compute_dtype=torch.bfloat16,
    llm_int8_enable_fp32_cpu_offload=True,
)

# Como agora CUDA_VISIBLE_DEVICES=1,0,
# o dispositivo 0 é a RTX 3060 (maior disponibilidade) e o 1 é a RTX 4070
max_memory = {
    1: "8GB",    # limite para a RTX 3060 (agora dispositivo 0)
    0: "8GB",    # limite para a RTX 4070
    "cpu": "80GB"
}

print("Carregar o modelo utilizando a nova configuração")
# Carregar o modelo utilizando a nova configuração
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    quantization_config=quantization_config,
    device_map="auto",  # distribui automaticamente entre GPUs e CPU
    torch_dtype=torch.float16,
    max_memory=max_memory,
    low_cpu_mem_usage=True,   # ajuda na carga de modelos muito grandes
    offload_folder="./offload"  # especifica o diretório para offload
)

# Carrega o dataset
dataset = load_dataset("ML2Healthcare/ClinicalTrialDataset")

# Função de tokenização
def tokenize_function(examples):
    return tokenizer(examples["data"], truncation=True, max_length=512)

# Aplicar a tokenização em batch
print("Aplicar a tokenização em batch")
tokenized_dataset = dataset.map(tokenize_function, batched=True)

training_args = TrainingArguments(
    output_dir="./results",
    num_train_epochs=3,  # number of training epochs
    per_device_train_batch_size=3,  # batch size per device during training
    gradient_accumulation_steps=2,  # number of steps before performing a backward/update pass
    gradient_checkpointing=True,  # use gradient checkpointing to save memory
    optim="adamw_torch_fused",  # use fused adamw optimizer
    logging_steps=10,  # log every 10 steps
    save_strategy="epoch",  # save checkpoint every epoch
    learning_rate=2e-4,  # learning rate, based on QLoRA paper
    bf16=True,  # use bfloat16 precision
    tf32=True,  # use tf32 precision
    max_grad_norm=0.3,  # max gradient norm based on QLoRA paper
    warmup_ratio=0.03,  # warmup ratio based on QLoRA paper
    lr_scheduler_type="constant",  # use constant learning rate scheduler
    deepspeed="ds_config.json",
    fp16=True,
    evaluation_strategy="no"
)
print("Chegou aqui")

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_dataset["train"],
    # Se houver split de validação, você pode incluir:
    # eval_dataset=tokenized_dataset["validation"]
)

# Inicia o treinamento
trainer.train()
