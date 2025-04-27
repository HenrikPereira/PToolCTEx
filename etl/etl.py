import pandas as pd
import duckdb as db
import numpy as np
import re
import ast

# ## Extraction
# data from scrapping relevant websites

ctis = pd.read_parquet('../scrapers/eu_ctr/data/ctis.parquet')

pap = pd.read_parquet('../scrapers/eu_ctr/data/pap.parquet')

trials = pd.read_parquet('../scrapers/eu_ctr/data/trials.parquet')


# data from the aact innitiative

con = db.connect()
con.execute("INSTALL postgres_scanner")

con.execute(
    '''
    LOAD postgres_scanner;
    SET pg_debug_show_queries = False;
    ATTACH '
        host=aact-db.ctti-clinicaltrials.org
        port=5432
        dbname=aact
        user=oliviaoliveira
        password=a123456B#
        connect_timeout=10
    ' AS aact (TYPE POSTGRES, READ_ONLY, SCHEMA ctgov);

    USE aact.ctgov;
    '''
)

cursor = con.cursor()

qry = '''
-- Query Principal para estudos em Portugal
WITH extra_info AS (
    SELECT *
    FROM aact.ctgov.id_information
    WHERE id_value ~ '^\d{4}-\d{6}-.*$' OR id_value IS NULL
),
terms AS (
     SELECT
         nct_id,
         array_agg(DISTINCT term) as terms,
         array_agg(DISTINCT st.group) as grouping
     FROM aact.ctgov.search_term_results str
    JOIN aact.ctgov.search_terms st on st.id = str.search_term_id
     GROUP BY nct_id
     ),
cond AS (
    SELECT
        nct_id,
        array_agg(DISTINCT name) as condition
    FROM aact.ctgov.conditions
    GROUP BY nct_id
),
elig AS (
    SELECT
        nct_id,
        array_agg(DISTINCT gender) as gender,
        array_agg(DISTINCT criteria) as criteria
    FROM aact.ctgov.eligibilities
    GROUP BY nct_id
),
key AS (
    SELECT
        nct_id,
        array_agg(DISTINCT name) as keys
    FROM aact.ctgov.keywords
    GROUP BY nct_id
),
inter AS (
    SELECT
        nct_id,
        array_agg(DISTINCT name) as interv
    FROM aact.ctgov.interventions
    GROUP BY nct_id
)


SELECT
    s.nct_id,
    i.id_value AS eudract_id,
    t.terms,
    t.grouping,
    cd.condition,
    official_title,
    acronym,
    phase,
    study_type,
    d.allocation,
    d.intervention_model,
    d.intervention_model_description,
    d.observational_model,
    d.primary_purpose,
    d.time_perspective,
    d.masking,
    d.masking_description,
    d.subject_masked,
    d.caregiver_masked,
    d.investigator_masked,
    d.outcomes_assessor_masked,
    overall_status,
    source,
    source_class,
    baseline_population,
    enrollment,
    enrollment_type,
    e.gender,
    cv.minimum_age_num,
    cv.minimum_age_unit,
    cv.maximum_age_num,
    cv.maximum_age_unit,
    number_of_arms,
    number_of_groups,
    inter.interv,
    e.criteria,
    k.keys,
    why_stopped,
    study_first_submitted_date,
    start_month_year,
    start_date,
    start_date_type,
    completion_month_year,
    completion_date,
    completion_date_type,
    has_expanded_access,
    expanded_access_nctid,
    expanded_access_status_for_nctid,
    expanded_access_type_individual,
    expanded_access_type_intermediate,
    expanded_access_type_treatment
FROM aact.ctgov.studies s
         JOIN aact.ctgov.countries AS c on s.nct_id = c.nct_id
         LEFT OUTER JOIN extra_info AS i on i.nct_id = s.nct_id
         LEFT OUTER JOIN terms AS t on t.nct_id = s.nct_id
         LEFT OUTER JOIN aact.ctgov.designs AS d on d.nct_id = s.nct_id
         LEFT OUTER JOIN cond AS cd on cd.nct_id = s.nct_id
         LEFT OUTER JOIN aact.ctgov.calculated_values AS cv on cv.nct_id = s.nct_id
         LEFT OUTER JOIN elig AS e on e.nct_id = s.nct_id
         LEFT OUTER JOIN key AS k on k.nct_id = s.nct_id
         LEFT OUTER JOIN inter on inter.nct_id = s.nct_id
WHERE c.name = 'Portugal'
  AND c.removed = false
ORDER BY study_first_submitted_date DESC;
'''

aact_cursor = cursor.execute(qry)

aact_df = aact_cursor.fetch_df()

# ## Transform
# ### Portuguese Trials from Clinical Trials EU
# Old and new databases

# #### Clean of old database

cols_w_dicts = ['trial_design', 'trial_scope', 'trial_phase']

# drop of columns with only null values
trials_clean = trials.copy().dropna(axis=0, how='all').dropna(axis=1, how='all')

# Convert stringified dictionaries into Python dictionaries and expand the 'trial_design' column
for col in cols_w_dicts:
    _temp = pd.json_normalize(trials_clean[col].apply(ast.literal_eval))
    _temp.columns = ['.'.join([col, i.strip().replace(' ', '_')]) for i in _temp.columns]
    _temp.infer_objects()
    trials_clean = (
        pd.concat([trials_clean, _temp], axis=1)
        .drop(columns=[col])
    )

# #### Clean of new database
# drop of columns with only null values
ctis_clean = ctis.copy().dropna(axis=0, how='all').dropna(axis=1, how='all')

cols_w_cols = ['Age', 'Gender']
# Convert stringified dictionaries into Python dictionaries and expand the 'trial_design' column
for col in cols_w_cols:
    _temp = ctis_clean[col].str.get_dummies(', ')
    _temp.columns = ['.'.join([col, i.strip().replace(' ', '_')]) for i in _temp.columns]
    _temp.infer_objects()
    ctis_clean = (
        pd.concat([ctis_clean, _temp], axis=1)
        .drop(columns=[col])
    )

phase_cols = ['Phase_I', 'Phase_II', 'Phase_III', 'Phase_IV']

# Create dummy columns by iterating through rows
for col in phase_cols:
    phase_pattern = r'\b' + re.escape(col.replace('_', ' ')) + r'\b'
    ctis_clean[col] = ctis_clean['trial_phase_desc'].str.contains(phase_pattern, regex=True).astype(int)

ctis_clean = ctis_clean.drop(columns=['trial_phase_desc'])

# #### Merge of trials database

def normalize_inner_duplicates(column):
    """
    Normaliza valores duplicados em células de uma coluna pandas. Remove duplicações textuais em strings separadas por vírgulas.

    Args:
        column (pd.Series): Coluna do DataFrame a ser normalizada.

    Returns:
        pd.Series: Coluna com valores duplicados normalizados.
    """
    return column.apply(
        lambda cell: ', '.join(dict.fromkeys(cell.split(', ')).keys()) if isinstance(cell, str) else cell
    )


trials_eu = (
    pd.concat([trials_clean, ctis_clean], axis=0)
    .sort_values(by='start_date', ascending=True)
    .assign(
        start_date=lambda x: pd.to_datetime(x['start_date'], format='%Y-%m-%d', errors='coerce'),
        end_date=lambda x: pd.to_datetime(x['end_date'], format='%Y-%m-%d', errors='coerce'),
        Sponsor=lambda x: normalize_inner_duplicates(x['Sponsor']).str.strip(),
        Sponsor_type=lambda x: normalize_inner_duplicates(x['Sponsor_type']).str.strip(),
        Age_0_17_years=lambda x: pd.to_numeric(x['Age.0-17_years'], errors='coerce')
        .fillna(pd.to_numeric(x['Age_Trial_has_subjects_under_18'], errors='coerce'))
        .fillna(pd.to_numeric(x['Age_Adolescents_(12-17_years)'], errors='coerce'))
        .fillna(pd.to_numeric(x['Age_Children_(2-11years)'], errors='coerce'))
        .fillna(pd.to_numeric(x['Age_Infants_and_toddlers_(28_days-23_months)'], errors='coerce'))
        .fillna(pd.to_numeric(x['Age_Newborns_(0-27_days)'], errors='coerce'))
        .fillna(pd.to_numeric(x['Age_Preterm_newborn_infants_(up_to_gestational_age_<_37_weeks)'], errors='coerce'))
        .astype('boolean'),
        Age_18_64_years=lambda x: pd.to_numeric(x['Age_Adults_(18-64_years)'], errors='coerce')
        .fillna(pd.to_numeric(x['Age.18-64_years'], errors='coerce'))
        .astype('boolean'),
        Age_65p_years=lambda x: pd.to_numeric(x['Age_Elderly_(>=65_years)'], errors='coerce')
        .fillna(pd.to_numeric(x['Age.65+_years'], errors='coerce'))
        .astype('boolean'),
        Gender_F=lambda x: pd.to_numeric(x['Gender_F'], errors='coerce')
        .fillna(pd.to_numeric(x['Gender.Female'], errors='coerce'))
        .astype('boolean'),
        Gender_M=lambda x: pd.to_numeric(x['Gender_M'], errors='coerce')
        .fillna(pd.to_numeric(x['Gender.Male'], errors='coerce'))
        .astype('boolean'),
        trial_design_Controlled=lambda x: pd.to_numeric(x['trial_design.Controlled'], errors='coerce').astype('boolean'),
        trial_design_Randomised=lambda x: pd.to_numeric(x['trial_design.Randomised'], errors='coerce').astype('boolean'),
        trial_design_Open=lambda x: pd.to_numeric(x['trial_design.Open'], errors='coerce').astype('boolean'),
        trial_design_Single_blind=lambda x: pd.to_numeric(x['trial_design.Single_blind'], errors='coerce').astype('boolean'),
        trial_design_Double_blind=lambda x: pd.to_numeric(x['trial_design.Double_blind'], errors='coerce').astype('boolean'),
        trial_design_Parallel_group=lambda x: pd.to_numeric(x['trial_design.Parallel_group'], errors='coerce').astype('boolean'),
        trial_design_Cross_over=lambda x: pd.to_numeric(x['trial_design.Cross_over'], errors='coerce').astype('boolean'),
        trial_design_Other_medicinal_product=lambda x: pd.to_numeric(x['trial_design.Other_medicinal_product(s)'], errors='coerce').astype('boolean'),
        trial_design_Placebo=lambda x: pd.to_numeric(x['trial_design.Placebo'], errors='coerce').astype('boolean'),
        trial_scope_Diagnosis=lambda x: pd.to_numeric(x['trial_scope.Diagnosis'], errors='coerce').astype('boolean'),
        trial_scope_Prophylaxis=lambda x: pd.to_numeric(x['trial_scope.Prophylaxis'], errors='coerce').astype('boolean'),
        trial_scope_Therapy=lambda x: pd.to_numeric(x['trial_scope.Therapy'], errors='coerce').astype('boolean'),
        trial_scope_Safety=lambda x: pd.to_numeric(x['trial_scope.Safety'], errors='coerce').astype('boolean'),
        trial_scope_Efficacy=lambda x: pd.to_numeric(x['trial_scope.Efficacy'], errors='coerce').astype('boolean'),
        trial_scope_Pharmacokinetic=lambda x: pd.to_numeric(x['trial_scope.Pharmacokinetic'], errors='coerce').astype('boolean'),
        trial_scope_Pharmacodynamic=lambda x: pd.to_numeric(x['trial_scope.Pharmacodynamic'], errors='coerce').astype('boolean'),
        trial_scope_Bioequivalence=lambda x: pd.to_numeric(x['trial_scope.Bioequivalence'], errors='coerce').astype('boolean'),
        trial_scope_Dose_response=lambda x: pd.to_numeric(x['trial_scope.Dose_response'], errors='coerce').astype('boolean'),
        trial_scope_Pharmacogenetic=lambda x: pd.to_numeric(x['trial_scope.Pharmacogenetic'], errors='coerce').astype('boolean'),
        trial_scope_Pharmacogenomic=lambda x: pd.to_numeric(x['trial_scope.Pharmacogenomic'], errors='coerce').astype('boolean'),
        trial_phase_First_administration_to_humans=lambda x: pd.to_numeric(x['trial_phase.First_administration_to_humans'], errors='coerce').astype('boolean'),
        trial_phase_Bioequivalence_study=lambda x: pd.to_numeric(x['trial_phase.Bioequivalence_study'], errors='coerce').astype('boolean'),
        trial_Phase_I=lambda x: pd.to_numeric(x['trial_phase.Human_pharmacology_(Phase_I)'], errors='coerce')
        .fillna(pd.to_numeric(x['Phase_I'], errors='coerce'))
        .astype('boolean')
        .fillna(False),
        trial_Phase_II=lambda x: pd.to_numeric(x['trial_phase.Therapeutic_exploratory_(Phase_II)'], errors='coerce')
        .fillna(pd.to_numeric(x['Phase_II'], errors='coerce'))
        .astype('boolean')
        .fillna(False),
        trial_Phase_III=lambda x: pd.to_numeric(x['trial_phase.Therapeutic_confirmatory_(Phase_III)'], errors='coerce')
        .fillna(pd.to_numeric(x['Phase_III'], errors='coerce'))
        .astype('boolean')
        .fillna(False),
        trial_Phase_IV=lambda x: pd.to_numeric(x['trial_phase.Therapeutic_use_(Phase_IV)'], errors='coerce')
        .fillna(pd.to_numeric(x['Phase_IV'], errors='coerce'))
        .astype('boolean')
        .fillna(False),
    )
)

cols_drop = [
    'Age_Preterm_newborn_infants_(up_to_gestational_age_<_37_weeks)',
    'Age_Newborns_(0-27_days)',
    'Age_Infants_and_toddlers_(28_days-23_months)',
    'Age_Children_(2-11years)',
    'Age_Adolescents_(12-17_years)',
    'Age_Adults_(18-64_years)',
    'Age_Elderly_(>=65_years)',
    'Age_Number_of_subjects_for_this_age_range:',
    'trial_design.The_trial_involves_single_site_in_the_Member_State_concerned',
    'trial_design.The_trial_involves_multiple_sites_in_the_Member_State_concerned',
    'trial_design.Number_of_sites_anticipated_in_Member_State_concerned',
    'trial_design.The_trial_involves_multiple_Member_States',
    'trial_design.Trial_being_conducted_both_within_and_outside_the_EEA',
    'trial_design.Trial_being_conducted_completely_outside_of_the_EEA',
    'trial_design.Trial_has_a_data_monitoring_committee',
    'trial_design.In_the_Member_State_concerned_years',
    'trial_design.In_the_Member_State_concerned_months',
    'trial_design.In_the_Member_State_concerned_days',
    'trial_design.In_all_countries_concerned_by_the_trial_years',
    'trial_design.In_all_countries_concerned_by_the_trial_months',
    'trial_design.In_all_countries_concerned_by_the_trial_days',
    'trial_design.Number_of_sites_anticipated_in_the_EEA',
    'trial_design.If_E.8.6.1_or_E.8.6.2_are_Yes,_specify_the_regions_in_which_trial_sites_are_planned',
    'trial_phase.Other_trial_type_description',
    'trial_design.Other_trial_design_description',
    'trial_scope.Others',
    'trial_scope.Other_scope_of_the_trial_description',
    'trial_phase',
    'Gender.Female',
    'Gender.Male',
    'trial_design.Controlled',
    'trial_design.Randomised',
    'trial_design.Open',
    'trial_design.Single_blind',
    'trial_design.Double_blind',
    'trial_design.Parallel_group',
    'trial_design.Cross_over',
    'trial_design.Other',
    'trial_design.Other_medicinal_product(s)',
    'trial_design.Comparator_of_controlled_trial',
    'trial_design.Placebo',
    'trial_scope.Diagnosis',
    'trial_scope.Prophylaxis',
    'trial_scope.Therapy',
    'trial_scope.Safety',
    'trial_scope.Efficacy',
    'trial_scope.Pharmacokinetic',
    'trial_scope.Pharmacodynamic',
    'trial_scope.Bioequivalence',
    'trial_scope.Dose_response',
    'trial_scope.Pharmacogenetic',
    'trial_scope.Pharmacoeconomic',
    'trial_scope.Pharmacogenomic',
    'trial_phase.Human_pharmacology_(Phase_I)',
    'trial_phase.First_administration_to_humans',
    'trial_phase.Bioequivalence_study',
    'trial_phase.Other',
    'trial_phase.Therapeutic_exploratory_(Phase_II)',
    'trial_phase.Therapeutic_confirmatory_(Phase_III)',
    'trial_phase.Therapeutic_use_(Phase_IV)',
    'Age.0-17_years',
    'Age.18-64_years',
    'Age.65+_years',
    'Phase_I',
    'Phase_II',
    'Phase_III',
    'Phase_IV',
    'Age_Trial_has_subjects_under_18',
    'Age_In_Utero',
] + [i for i in trials_eu.columns if i.startswith('trial_design.Definition_of_the_end_of_the_trial_and_justification_where')]

sponsor_dict = trials_eu.dropna(subset='Sponsor_type').groupby('Sponsor').agg({'Sponsor_type': lambda x: ', '.join(set((', '.join(set(x))).split(', ')))}).drop_duplicates()

trials_eu = (
    trials_eu
    .drop(columns=cols_drop)
    .assign(
        Sponsor_type=lambda x: x['Sponsor'].map(sponsor_dict['Sponsor_type'])  # Está correcto?
    )
)

# ### Clinical Trials .gov
def normalize_age_to_years_series(num_series, unit_series):
    # Converte valores com base na unidade
    years = np.where(unit_series == 'Years', num_series,  # Se unidade for anos, mantém
                     np.where(unit_series == 'Months', num_series / 12,  # Se meses, divide por 12
                              np.where(unit_series == 'Days', num_series / 365,
                                       np.nan)))  # Se dias, divide por 365, senão NaN
    return pd.Series(years, index=num_series.index)


aact_df_clean = (
    pd.concat([
        aact_df,
        pd.get_dummies(aact_df['phase']),
        pd.get_dummies(aact_df['allocation'], prefix='allocation'),
        pd.get_dummies(aact_df['intervention_model'], prefix='intervention_model'),
        pd.get_dummies(aact_df['masking'], prefix='masking'),
        pd.get_dummies(aact_df['gender'].apply(lambda x: ' | '.join(map(str, x)) if isinstance(x, (list, tuple)) else pd.NA),prefix='Gender'),
    ], axis=1)
    .dropna(axis=0, how='all')
    .dropna(axis=1, how='all')
    .rename(columns={
        'eudract_id': 'eudract_nr',
        'official_title': 'title',
        'acronym': 'Protocol',
    })
    .assign(
        terms=lambda x: x['terms'].apply(lambda groupn: ' | '.join(map(str, groupn)) if isinstance(groupn, (list, tuple)) else groupn),
        grouping=lambda x: x['grouping'].apply(lambda groupn: ' | '.join(map(str, groupn)) if isinstance(groupn, (list, tuple)) else groupn),
        condition=lambda x: x['condition'].apply(lambda cond: ' | '.join(map(str, cond)) if isinstance(cond, (list, tuple)) else cond),
        study_type=lambda x: x['study_type'].astype('category'),
        intervention_model=lambda x: x['intervention_model'].astype('category'),
        intervention_model_description=lambda x: x['intervention_model_description'].astype('category'),
        observational_model=lambda x: x['observational_model'].astype('category'),
        primary_purpose=lambda x: x['primary_purpose'].astype('category'),
        time_perspective=lambda x: x['time_perspective'].astype('category'),
        masking=lambda x: x['masking'].astype('category'),
        overall_status=lambda x: x['overall_status'].astype('category'),
        source_class=lambda x: x['source_class'].astype('category'),
        enrollment_type=lambda x: x['enrollment_type'].astype('category'),
        trial_Phase_I=lambda x: x['PHASE1'].fillna(x['PHASE1/PHASE2']).astype('boolean'),
        trial_Phase_II=lambda x: x['PHASE2'].fillna(x['PHASE1/PHASE2']).astype('boolean'),
        trial_Phase_III=lambda x: x['PHASE3'].fillna(x['PHASE2/PHASE3']).astype('boolean'),
        trial_Phase_IV=lambda x: x['PHASE4'].astype('boolean'),
        maximum_age_num=lambda x: normalize_age_to_years_series(x['maximum_age_num'], x['maximum_age_unit']),
        minimum_age_num=lambda x: normalize_age_to_years_series(x['minimum_age_num'], x['minimum_age_unit']),
        allocation_RANDOMIZED=lambda x: x['allocation_RANDOMIZED'].astype('boolean'),
        allocation_NON_RANDOMIZED=lambda x: x['allocation_NON_RANDOMIZED'].astype('boolean'),
    )
    .drop(columns=[
        'phase',
        'NA',
        'PHASE1',
        'PHASE2',
        'PHASE1/PHASE2',
        'PHASE3',
        'PHASE2/PHASE3',
        'PHASE4',
        'minimum_age_unit',
        'maximum_age_unit',
        'allocation_NA',
        'allocation',
        'Gender_',
        'gender',
    ], errors='ignore')
)


# ### Final Merge
import json

# Função para converter o conteúdo da célula em uma lista
def transformar_em_lista(valor):
    if pd.isna(valor):  # Se o valor for NaN, retorna uma lista vazia
        return []
    # Se o valor já for uma lista, retorna-o
    if isinstance(valor, list):
        return valor
    try:
        # Tenta converter a string que representa a lista para o objeto lista
        return ast.literal_eval(valor)
    except Exception:
        # Caso a conversão falhe, usa split
        return [item.strip() for item in valor.split(",") if item.strip()]

def extract_criteria(text, type='inclusion'):
    if pd.isna(text):
        return pd.NA

    # Se já for lista ou np.ndarray, usar o primeiro elemento
    if isinstance(text, (list, np.ndarray)):
        text = text[0]

    text = str(text).strip()

    # Padrões claros para inclusão e exclusão
    inclusion_pattern = r'Inclusion Criteria.*?:\s*(.*?)(?=Exclusion Criteria|$)'
    exclusion_pattern = r'Exclusion Criteria.*?:\s*(.*)$'

    pattern = inclusion_pattern if type == 'inclusion' else exclusion_pattern
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)

    if match:
        criteria_text = match.group(1).strip()
    else:
        return [pd.NA]

    # Limpeza e formatação padrão
    criteria_text = re.sub(r'\r\n|\r|\n', '; ', criteria_text)  # substituir quebra de linhas por "; "
    criteria_text = re.sub(r'[\*\•\\]', '', criteria_text)      # remover caracteres especiais
    criteria_text = re.sub(r'(\\>|\\<|≥|≤|>=|<=)', ' ', criteria_text)  # remover símbolos especiais
    criteria_text = re.sub(r'\s{2,}', ' ', criteria_text)       # normalizar espaços extras
    criteria_text = criteria_text.strip('; ')

    # Dividir criterios individuais com base em números ou ";"
    criteria_list = re.split(r';\s*|\d+\.\s+', criteria_text)
    criteria_list = [crit.strip() for crit in criteria_list if crit.strip()]

    return criteria_list if criteria_list else [pd.NA]

def ensure_list_format(keys):
    if isinstance(keys, np.ndarray):
        return keys.tolist()
    elif isinstance(keys, list):
        return keys
    elif pd.isna(keys):
        return pd.NA
    else:
        return [str(keys).strip()]

def clean_therapeutic_area(row):
    areas = []

    for col in ['therapeutic_area', 'terms', 'grouping', 'condition']:
        val = row.get(col)

        # Ignora valores nulos diretamente
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue

        # Se for array, list ou tuple → junta como string
        if isinstance(val, (np.ndarray, list, tuple)):
            val = ' | '.join(map(str, val))

        # Tentar interpretar como lista (se for string com aspas)
        parsed = None
        if isinstance(val, str):
            try:
                parsed = ast.literal_eval(val)
            except:
                parsed = None

        # Se parsed funcionar e for lista
        if isinstance(parsed, list):
            areas.extend(parsed)
        elif isinstance(val, str):
            # Se não for interpretável como lista, tenta split
            if '|' in val:
                areas.extend([t.strip() for t in val.split('|')])
            elif ',' in val:
                areas.extend([t.strip() for t in val.split(',')])
            else:
                areas.append(val.strip())

    # Limpezas finais
    areas = [
        re.sub(r'\(.*\)', '', area.replace('Diseases [C] - ', '').replace("'", '').lower()).strip()
        for area in areas if isinstance(area, str)
    ]
    areas = [re.sub(r'\[[a-z]+\d+\]', '', area).strip() for area in areas]
    areas = [area.replace('neoplasms', 'neoplasm') for area in areas]

    # Remover duplicados mantendo ordem
    seen = set()
    areas_unique = []
    for area in areas:
        if area not in seen and area:
            seen.add(area)
            areas_unique.append(area)

    return pd.NA if not areas_unique else areas_unique

full = (
    pd.merge(trials_eu, aact_df_clean, on='eudract_nr', how='outer')
    .assign(
        title=lambda x: x['title_y'].fillna(x['title_x']),
        Protocol=lambda x: x['Protocol_y'].fillna(x['Protocol_x']),
        start_date=lambda x: x[['start_date_x', 'start_date_y']].min(axis=1).fillna(x['start_date_x'].replace(False, pd.NA)).fillna(x['start_date_y'].replace(False, pd.NA)),
        end_date=lambda x: x[['end_date', 'completion_date']].max(axis=1).fillna(x['end_date'].replace(False, pd.NA)).fillna(x['completion_date'].replace(False, pd.NA)),
        trial_Early_Phase_I=lambda x: x['EARLY_PHASE1'].fillna(False).astype('boolean'),
        trial_Phase_I=lambda x: x['trial_Phase_I_y'].fillna(x['trial_Phase_I_x']).astype('boolean'),
        trial_Phase_II=lambda x: x['trial_Phase_II_y'].fillna(x['trial_Phase_II_x']).astype('boolean'),
        trial_Phase_III=lambda x: x['trial_Phase_III_y'].fillna(x['trial_Phase_III_x']).astype('boolean'),
        trial_Phase_IV=lambda x: x['trial_Phase_IV_y'].fillna(x['trial_Phase_IV_x']).astype('boolean'),
        condition=lambda x: x['condition_y'].fillna(x['condition_x']),
        therapeutic_area=lambda x: x.apply(clean_therapeutic_area, axis=1),
        keywords=lambda x: x['keys'].apply(ensure_list_format),
        interventions=lambda x: x['interv'].apply(ensure_list_format),
        Sponsor=lambda x: x['Sponsor'].fillna(x['source']),
        Sponsor_type=lambda x: x['Sponsor_type'].fillna(x['source_class']),
        status=lambda x: x['status'].fillna(x['overall_status'].astype(str)).astype(str).astype('category'),
        inclusion_crt=lambda x: x.apply(lambda r: r['inclusion_crt'] if pd.notna(r['inclusion_crt']) else extract_criteria(r['criteria'], type='inclusion'), axis=1),
        exclusion_crt=lambda x: x.apply(lambda r: r['exclusion_crt'] if pd.notna(r['exclusion_crt']) else extract_criteria(r['criteria'], type='exclusion'), axis=1),
        enrollment=lambda x: x['enrollment'].fillna(x['nr_enrolled']),
        Gender_F=lambda x: x['Gender_F'].fillna(x['Gender_FEMALE'] if 'Gender_FEMALE' in x.columns else False).astype('boolean'),
        Gender_M=lambda x: x['Gender_M'].fillna(x['Gender_MALE'] if 'Gender_MALE' in x.columns else False).astype('boolean'),
        Age_0_17_years=lambda x: ((x['minimum_age_num'] <= 17) & (x['maximum_age_num'] >= 0)),
        Age_18_64_years=lambda x: ((x['maximum_age_num'] <= 64) & (x['minimum_age_num'] >= 18)),
        Age_65p_years=lambda x: (x['minimum_age_num'] >= 65),
        trial_design_Randomised=lambda x: x['trial_design_Randomised'].fillna(x['allocation_RANDOMIZED'].replace(False, pd.NA)).fillna(~x['allocation_NON_RANDOMIZED']),
        trial_design_Parallel_group=lambda x: x['trial_design_Parallel_group'].fillna(x['intervention_model_PARALLEL']),
        trial_design_Cross_over=lambda x: x['trial_design_Cross_over'].fillna(x['intervention_model_CROSSOVER']),
        trial_design_Controlled=lambda x: x['trial_design_Controlled'].fillna(~x['intervention_model_SINGLE_GROUP'].astype('boolean')),
        masking_OPEN=lambda x: x['masking_NONE'].fillna(x['trial_design_Open']),
        masking_SINGLE=lambda x: x['masking_SINGLE'].fillna(x['trial_design_Single_blind']),
        masking_DOUBLE=lambda x: x['masking_DOUBLE'].fillna(x['trial_design_Double_blind']),
        number_of_arms=lambda x: x['number_of_arms'].fillna(pd.to_numeric(x['trial_design.Number_of_treatment_arms_in_the_trial'], errors='coerce')),
    )
    .drop(columns=[
        'title_x',
        'title_y',
        'Protocol_x',
        'Protocol_y',
        'start_date_x',
        'start_date_y',
        'completion_date',
        'end_date',
        'trial_Phase_I_x',
        'trial_Phase_I_y',
        'trial_Phase_II_x',
        'trial_Phase_II_y',
        'trial_Phase_III_x',
        'trial_Phase_III_y',
        'trial_Phase_IV_x',
        'trial_Phase_IV_y',
        'EARLY_PHASE1',
        'condition_x',
        'condition_y',
        'source',
        'source_class',
        'overall_status',
        'nr_enrolled',
        'minimum_age_num',
        'maximum_age_num',
        'Gender_ALL',
        'Gender_FEMALE',
        'Gender_MALE',
        'allocation_RANDOMIZED',
        'allocation_NON_RANDOMIZED',
        'intervention_model_PARALLEL',
        'masking_NONE',
        'masking_description',
        'masking',
        'trial_design_Open',
        'trial_design_Single_blind',
        'trial_design_Double_blind',
        'intervention_model_CROSSOVER',
        'trial_design.Number_of_treatment_arms_in_the_trial',
        'terms',
        'grouping',
        'criteria',
        'condition',
        'keys',
        'interv',
    ], errors='ignore')
    .sort_values(by='start_date', ascending=False)
)

def safe_serialize(obj):
    if obj is pd.NA or (isinstance(obj, float) and pd.isna(obj)):
        return None
    if isinstance(obj, (list, tuple, np.ndarray)):
        return [safe_serialize(o) for o in obj]
    return obj


def save_df_parquet(df, path):
    def convert_value(x):
        if isinstance(x, (list, tuple, np.ndarray)):
            safe_value = safe_serialize(x)
            return "__list__" + json.dumps(safe_value)
        elif x is pd.NA or (isinstance(x, float) and pd.isna(x)):
            return None
        return x

    df_converted = df.applymap(convert_value)
    df_converted.to_parquet(path)


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

    return df.applymap(revert_value)


# Coluna enrollment é um object que contém mais do que um tipo de dados ('float' e 'str')

full['enrollment'] = pd.to_numeric(full['enrollment'], errors='coerce')

# Saber se o estudo veio da ClinicalTrials.gov, ClinicalTrials EU

def infer_source(row):
    if pd.notna(row.get('nct_id')):
        return 'clinicaltrials.gov'
    elif pd.notna(row.get('source')) or pd.notna(row.get('Sponsor_type')):
        return 'clinicaltrials.eu'
    elif pd.notna(row.get('pap_id')):  # Exemplo, muda conforme a estrutura do PAP
        return 'pap.infarmed'
    return 'unknown'

full['source_dataset'] = full.apply(infer_source, axis=1)

save_df_parquet(full, '../sources/full_df.parquet')
full.to_excel('data/full_merge.xlsx', index=False)

pap_clean = pap.copy()

# Normalizar colunas e datas
pap_clean = pap_clean.rename(columns={
    'Nome': 'title',
    'DCI': 'intervention',
    'data_decisao': 'start_date',
    'detalhes': 'condition',
    'n_doentes': 'enrollment'
})

# Converter datas
pap_clean['start_date'] = pd.to_datetime(pap_clean['start_date'], format="%d/%m/%Y", errors='coerce')

# Garantir valores numéricos
pap_clean['enrollment'] = pd.to_numeric(pap_clean['enrollment'], errors='coerce')

# Preencher colunas que existem no full com pd.NA para garantir alinhamento
columns_in_full = full.columns
for col in columns_in_full:
    if col not in pap_clean.columns:
        pap_clean[col] = pd.NA

# Adicionar coluna de source
pap_clean['source_dataset'] = 'PAP Infarmed'

# Reordenar colunas para corresponder à estrutura do full
pap_clean = pap_clean[columns_in_full]
