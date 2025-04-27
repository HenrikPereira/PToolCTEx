import json
import tomllib
import streamlit as st
import pandas as pd
import math
from groq import Groq

# function to load extra options and overrides
@st.cache_data
def load_extras():
    with open(".streamlit/config.toml", "rb") as f:
        _config = tomllib.load(f)
    with open(".streamlit/options.toml", "rb") as f:
        _options = tomllib.load(f)

    return _config, _options

def parse_list_str(val):
    """
    Transforma strings tipo '__list__["abc", "def"]' em listas reais.
    """
    if isinstance(val, str) and val.startswith("__list__"):
        try:
            return json.loads(val.replace("__list__", ""))
        except Exception:
            return [val]
    return val

@st.cache_data
def load_df_parquet(path):
    df = pd.read_parquet(path)

    def revert_value(x):
        if isinstance(x, str) and x.startswith("__list__"):
            try:
                return json.loads(x[len("__list__"):])
            except Exception:
                return x
        if pd.isna(x):
            return pd.NA
        return x

    return df.map(revert_value)

@st.cache_data
def get_groq_models():
    import os
    import requests

    try:
        api_key = st.secrets.get('GROQ', '').get('API_KEY', '')
        url = st.secrets.get('GROQ', '').get('model_list_url', '')

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers)

        return pd.json_normalize(response.json().get("data", [])).query('context_window > 9100').filter([
            'id', 'owned_by', 'active', 'context_window', 'max_completion_tokens'
        ]).sort_values('id', ascending=False)
    except Exception as e:
        print(e)
        return None


def prepare_trials_generator(trials_df, chunk_size=50, type_trials='recent', proportion=0.5):
    """
    Gera blocos de contexto com até `chunk_size` ensaios de cada vez,
    prontos para serem enviados ao Grok separadamente.

    :param trials_df: pd.DataFrame com os ensaios clínicos
    :param chunk_size: máximo de linhas por bloco
    :param type_trials: 'recent' para ordenar por start_date desc,
                        'sample' para embaralhar, outro valor para ordem original
    :yields: string de contexto com até chunk_size trials
    """
    # 1. Seleciona e ordena/amostra o DataFrame
    if type_trials == 'sample':
        df = trials_df.sample(frac=1, random_state=123)
    elif type_trials == 'recent':
        df = trials_df.sort_values('start_date', ascending=False)
    else:
        df = trials_df

    total = len(df)
    n_chunks = math.ceil(total / chunk_size)
    print(f'Total de linhas: {total}. Processadas em {n_chunks} partes de {chunk_size} linhas.')

    checkpoint = math.ceil(n_chunks * proportion)

    # 2. Para cada fatia, gera o texto
    for i in range(checkpoint):
        start = i * chunk_size
        end = start + chunk_size
        chunk = df.iloc[start:end, :]
        print(f'Inicio em id {start}, id fim {end}, com número de linhas: {chunk.shape[0]}')

        context = "List of clinical trials to be filtered by context:\n"
        for idx, row in chunk.iterrows():
            context += (
                f"- id: {idx}, "
                f"Title: {row.get('title', 'N/D')}\n"
            )
        yield context


def prepare_trials_context(trials_df, max_trials=1000, type_trials='recent'):
    """
    Prepara um resumo dos ensaios clínicos a partir do dataset.
    Aqui estamos apenas utilizando os 5 primeiros registros para não sobrecarregar a mensagem.
    Poderás adaptar essa função para selecionar os ensaios que considerares mais relevantes.
    """
    context = "Short list of clinical trials:\n"

    if type_trials == 'sample':
        _generator = trials_df.sample(max_trials, random_state=123).iterrows()
    elif type_trials == 'recent':
        _generator = trials_df.sort_values('start_date', ascending=False).head(max_trials).iterrows()
    else:
        context += '(empty)'
        return context

    for idx, row in _generator:
        # Caso tenhas um título ou protocol, poderás incluir mais informações
        context += (
            f"- id: {idx}, "
            f"Title: {row.get('title', 'N/D')}, "
            f"Therapeutic area: {row.get('therapeutic_area', 'N/D')}, "
            f"Keywords: {row.get('keywords', 'N/D')}, "
            f"Inclusion: {row.get('inclusion_crt', 'N/D')},"
            f"Exclusion: {row.get('exclusion_crt', 'N/D')}\n"
        )
    return context


def format_user_prompt_template(prompt):
    return f'''
        Use information from following user prompt to construct the proper JSON output: {prompt}
    '''

def format_system_prefilter_role_template(trials):
    return f'''
            You are a medical assistant API specialized in health Clinical Trials that returns only JSON outputs. 
            The JSON schema should use the folowing (clear of any escape characters): 
            {{"database_index": "integer (the index of the trial in the database)", 
            "certainty": "float (the probability of the trial being relevant to the prompt)"}}. 
            
            Your job is to analyse a user prompt for its clinical context and, from the provided database, 
            return the best matches (certainty above 0.5), where possible, of eligible 
            clinical trials in JSON format ONLY for that context. The Clinical Trial Database: {trials}
        '''


def format_system_final_role_template(trials_context):
    return f'''
        You are a medical assistant API specialized in health Clinical Trials that returns only JSON outputs. 
        Your job is to analyse a user prompt with a clinical context and, from the provided database, 
        return the top 5 matches of eligible clinical trials in JSON format for that context. 
        The JSON schema should use the folowing (clear of any escape characters): 
        {{"database_index": "integer (the index of the trial in the database)", 
        "certainty": "float (the probability of the trial being relevant to the prompt)", 
        "Title": "string (the title of the trial)"}}. 
        Be aware of the inclusion and exclusion criteria of the trials when calculating certainty.
        The Clinical Trial Database details: {trials_context}
    '''

@st.cache_data(show_spinner=True)
def get_trial_recommendation_groq(
        prompt,
        trials_df,
        chunk_size=100,
        type_trials='recent',
        proportion=0.5,
        certainty_cutoff=0.5
):
    """
    Utiliza a API da Groq para enviar uma mensagem de reasoning que combine os dados dos ensaios clínicos
    com o prompt do utilizador, devolvendo a recomendação.
    """
    # Prepara o contexto dos ensaios clínicos a incluir na mensagem
    prefiltered = []
    counter = 0

    user_prompt_template = format_user_prompt_template(prompt)

    for trials in prepare_trials_generator(
            trials_df,
            chunk_size=chunk_size,
            type_trials=type_trials,
            proportion=proportion
    ):
        # trials = prepare_trials_prefilter(trials_df, max_trials, type_trials)
        print('Tamanho do texto de trials', len(trials))

        system_prompt_prefilter = format_system_prefilter_role_template(trials)

        # Inicializa o cliente Groq e envia a requisição conforme a documentação
        client = Groq(api_key=st.secrets.get('GROQ', '').get('API_KEY'))
        model = st.session_state.prefilter_model

        completion_filter = client.chat.completions.create(
            model=model,
            messages = [
                {
                    "role": "system",
                    "content": system_prompt_prefilter
                },
                {
                    "role": "user",
                    "content": user_prompt_template,
                }
            ],
            # response_format = {"type": "json_object"}, # Add this response format to configure JSON mode
            temperature=0.1,
            seed=123,
        )

        parte = completion_filter.choices[0].message.content.split('```')[-2]
        json_str = parte.strip().replace('json', '')

        try:
            pref = pd.DataFrame(json.loads(json_str))
            if pref.shape[0] > 0:
                prefiltered.append(pd.DataFrame(json.loads(json_str)))
                counter += pref.shape[0]
        except Exception as e:
            print(e)
            print(json_str)
            continue

    df = trials_df.loc[(
        pd.concat(prefiltered)
        .query('certainty > @certainty_cutoff')
    ).database_index, :]

    trials_context = prepare_trials_context(df, max_trials=len(df), type_trials=type_trials)

    try:
        system_prompt_final = format_system_final_role_template(trials_context)

        # Inicializa o cliente Groq e envia a requisição conforme a documentação
        client = Groq(api_key=st.secrets.get('GROQ', '').get('API_KEY'))
        model=st.session_state.final_model

        completion = client.chat.completions.create(
            model=model,
            messages = [
                {
                    "role": "system",
                    "content": system_prompt_final
                },
                {
                    "role": "user",
                    "content": user_prompt_template,
                }
            ],
            # response_format = {"type": "json_object"}, # Add this response format to configure JSON mode
            temperature=1,
            seed=123,
        )

        parte = completion.choices[0].message.content.split('```')[-2]
        json_str = parte.strip().replace('json', '')

        try:
            pref = pd.DataFrame(json.loads(json_str))
            if pref.shape[0] > 0:
                return pref
            return None
        except Exception as e:
            print(e)
            return None
    except Exception as e:
        print(e)
        return df, trials_context