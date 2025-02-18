# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PAPItem(scrapy.Item):
    Nome = scrapy.Field()

    DCI = scrapy.Field()

    decisao = scrapy.Field()

    data_decisao = scrapy.Field()

    detalhes = scrapy.Field() # População alvo, indicação terapêutica aprovada ou indiferida

    n_doentes = scrapy.Field()

    cond_observ = scrapy.Field()


class TrialItem(scrapy.Item):
    # Define o título do ensaio clínico
    title = scrapy.Field()
    # Define o identificador do ensaio (por exemplo, EudraCT Number)
    eudract_nr = scrapy.Field()
    nct_nr = scrapy.Field()
    trial_phase = scrapy.Field()
    trial_phase_desc = scrapy.Field()
    trial_design = scrapy.Field()
    trial_scope = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    Protocol = scrapy.Field()
    Sponsor = scrapy.Field()
    Sponsor_type = scrapy.Field()
    therapeutic_area = scrapy.Field()
    condition = scrapy.Field()
    Disease = scrapy.Field()
    Age = scrapy.Field()
    Gender = scrapy.Field()
    inclusion_crt = scrapy.Field()
    exclusion_crt = scrapy.Field()
    nr_enrolled = scrapy.Field()
    # Armazena a URL da página de detalhe do ensaio clínico
    url = scrapy.Field()
    status = scrapy.Field()
    # Define um campo para armazenar outros detalhes extraídos (como um dicionário)
    details = scrapy.Field()

