import json
import scrapy

from ..items import TrialItem  # ou o item que desejar, se necessário

class CtisEuSpider(scrapy.Spider):
    name = "ctis_eu"
    allowed_domains = ["euclinicaltrials.eu"]
    start_urls = []

    # Defina os headers e cookies como atributos para reutilizá-los nas requisições
    custom_headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
        'content-type': 'application/json',
        'dnt': '1',
        'origin': 'https://euclinicaltrials.eu',
        'priority': 'u=1, i',
        'referer': 'https://euclinicaltrials.eu/ctis-public/search?lang=en',
        'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
    }

    custom_cookies = {
        'accepted_cookie': 'true'
    }

    record_nr = 1
    report_on = 20

    def start_requests(self):
        # Inicia a requisição para a primeira página (page=1)
        payload = self.build_payload(page=1)
        yield scrapy.Request(
            url='https://euclinicaltrials.eu/ctis-public-api/search',
            method='POST',
            body=json.dumps(payload),
            headers=self.custom_headers,
            cookies=self.custom_cookies,
            meta={"cookiejar": 1, "page": 1},
            callback=self.parse_endpoint
        )

    def build_payload(self, page):
        """Retorna o payload com o número de página atualizado."""
        return {
            "pagination": {
                "page": page,
                "size": 100
            },
            "sort": {
                "property": "decisionDate",
                "direction": "DESC"
            },
            "searchCriteria": {
                "containAll": None,
                "containAny": None,
                "containNot": None,
                "title": None,
                "number": None,
                "status": None,
                "medicalCondition": None,
                "sponsor": None,
                "endPoint": None,
                "productName": None,
                "productRole": None,
                "populationType": None,
                "orphanDesignation": None,
                "msc": [620],
                "ageGroupCode": None,
                "therapeuticAreaCode": None,
                "trialPhaseCode": None,
                "sponsorTypeCode": None,
                "gender": None,
                "eeaStartDateFrom": None,
                "eeaStartDateTo": None,
                "eeaEndDateFrom": None,
                "eeaEndDateTo": None,
                "protocolCode": None,
                "rareDisease": None,
                "pip": None,
                "haveOrphanDesignation": None,
                "hasStudyResults": None,
                "hasClinicalStudyReport": None,
                "isLowIntervention": None,
                "hasSeriousBreach": None,
                "hasUnexpectedEvent": None,
                "hasUrgentSafetyMeasure": None,
                "isTransitioned": None,
                "eudraCtCode": None,
                "trialRegion": None,
                "vulnerablePopulation": None,
                "mscStatus": None
            }
        }

    def parse_endpoint(self, response):
        self.logger.info("Página renderizada e interações realizadas com sucesso.")
        json_response = response.json()

        # Inicializa a lista de ctNumbers na primeira requisição
        if not hasattr(self, 'dict_dados'):
            self.dict_dados = {}

        dados = json_response.get('data', [])
        # Adiciona os ctNumber de cada registro ao dicionário
        for registo in dados:
            if 'ctNumber' in registo:
                self.dict_dados[registo['ctNumber']] = registo

        # Verifica se há mais páginas (assumindo que o JSON possui uma chave 'pagination')
        pagination = json_response.get('pagination', {})
        current_page = pagination.get('currentPage', 1)
        total_pages = pagination.get('totalPages', current_page)

        self.logger.info("Página %d de %d processada.", current_page, total_pages)

        if current_page < total_pages:
            next_page = current_page + 1
            payload = self.build_payload(page=next_page)
            yield scrapy.Request(
                url='https://euclinicaltrials.eu/ctis-public-api/search',
                method='POST',
                body=json.dumps(payload),
                headers=self.custom_headers,
                cookies=self.custom_cookies,
                meta={"cookiejar": response.meta.get("cookiejar", 1), "page": next_page},
                callback=self.parse_endpoint
            )
        else:
            # Se todas as páginas foram processadas, emite a lista completa
            self.logger.info("Todas as páginas processadas. Total de ctNumbers: %d", len(self.dict_dados.keys()))
            # Agora, para cada ctNumber, gere uma requisição para o endpoint de retrieve.
            for ctnumber in self.dict_dados.keys():
                retrieve_url = f"https://euclinicaltrials.eu/ctis-public-api/retrieve/{ctnumber}"
                yield scrapy.Request(
                    url=retrieve_url,
                    method="GET",
                    headers=self.custom_headers,
                    cookies=self.custom_cookies,
                    callback=self.parse_retrieve
                )

    def parse_retrieve(self, response):
        if self.record_nr % self.report_on == 0:
            self.logger.info(f"Recebidos {self.record_nr + 1} registos completos.")

        try:
            data = response.json()
        except Exception as e:
            self.logger.error("Erro ao converter resposta para JSON: %s", e)
            data = {}

        _dict = self.dict_dados[data.get('ctNumber', '')]

        # mapear os dados para um TrialItem
        item = TrialItem()

        item['title'] = _dict.get('ctTitle', '')
        item['eudract_nr'] = _dict.get('ctNumber', '')
        item['nct_nr'] = None
        item['trial_phase'] = data[
            'authorizedApplication'][
            'authorizedPartI'][
            'trialDetails'][
            'trialInformation'][
            'trialCategory'][
            'trialPhase']
        item['trial_phase_desc'] = _dict.get('trialPhase', '')
        item['trial_design'] = None
        item['trial_scope'] = None
        item['start_date'] = data[
            'authorizedApplication'][
            'authorizedPartI'][
            'trialDetails'][
            'trialInformation'][
            'trialDuration'][
            'estimatedRecruitmentStartDate']
        item['end_date'] = data[
            'authorizedApplication'][
            'authorizedPartI'][
            'trialDetails'][
            'trialInformation'][
            'trialDuration'][
            'estimatedEndDate']
        item['Protocol'] = _dict.get('shortTitle', '')
        item['Sponsor'] = _dict.get('sponsor', '')
        item['Sponsor_type'] = _dict.get('sponsorType', '')
        item['therapeutic_area'] = json.dumps(_dict.get('therapeuticAreas', ''))
        item['condition'] = _dict.get('conditions', '')
        item['Disease'] = None
        item['Age'] = _dict.get('ageGroup', '')
        item['Gender'] = _dict.get('gender', '')

        trial_info = data.get('authorizedApplication', {}) \
            .get('authorizedPartI', {}) \
            .get('trialDetails', {}) \
            .get('trialInformation', {}) \
            .get('eligibilityCriteria', {}) \
            .get('principalInclusionCriteria', [])
        item['inclusion_crt'] = json.dumps([
            d.get('principalInclusionCriteria', '')
            for d in trial_info
        ]) if len(trial_info) > 0 else None
        trial_info = data.get('authorizedApplication', {}) \
            .get('authorizedPartI', {}) \
            .get('trialDetails', {}) \
            .get('trialInformation', {}) \
            .get('eligibilityCriteria', {}) \
            .get('principalExclusionCriteria', [])
        item['exclusion_crt'] = json.dumps([
            d.get('principalExclusionCriteria', '')
            for d in trial_info
        ]) if len(trial_info) > 0 else None

        item['nr_enrolled'] = _dict.get('totalNumberEnrolled', '')
        item['url'] = response.url
        item['status'] = _dict.get('ctStatus', '')

        self.record_nr += 1
        yield item
