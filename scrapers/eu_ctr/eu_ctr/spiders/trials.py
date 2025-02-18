import json
from urllib.parse import urljoin

import scrapy
from ..items import TrialItem


class TrialsSpider(scrapy.Spider):
    name = "trials"
    allowed_domains = ["clinicaltrialsregister.eu"]
    start_urls = ["https://www.clinicaltrialsregister.eu/ctr-search/search?query=&country=pt"]

    def parse(self, response, **kwargs):
        """
        Faz o parsing da página de resultados:
          - Extrai os links para os detalhes de cada ensaio clínico.
          - Trata a paginação, se houver, seguindo o link para a próxima página.
          :param response:
        """
        # Exemplo: supondo que os resultados estejam numa tabela com a classe "result"
        rows = response.xpath('//table[contains(@class, "result")]//tr')

        for row in rows:
            relative_link = row.xpath(".//a[contains(text(), 'PT')]/@href").get()
            if relative_link:
                trial_url = response.urljoin(relative_link)
                yield scrapy.Request(trial_url, callback=self.parse_trial)

        # Tratamento da paginação: busca por um link para a próxima página (exemplo)
        next_page = response.xpath('//a[contains(., "Next»")]/@href').get()
        if next_page:
            next_page_url = urljoin(response.url, ''.join(['search?query=&country=pt', next_page]))  # response.urljoin(next_page)
            print(next_page_url)
            yield scrapy.Request(next_page_url, callback=self.parse)

    @staticmethod
    def parse_trial(response):
        item = TrialItem()

        item['title'] = "\n".join(response.xpath('//tr[td[1][normalize-space()="A.3"]]/td[3]/table//td/text()').getall()).strip()
        item['eudract_nr'] = response.xpath('//tr[td[1][normalize-space()="A.2"]]/td[3]//text()').get().strip()
        item['nct_nr'] = response.xpath('//tr[td[1][contains(., "NCT")]]/td[3]//text()').get()
        item['trial_design'] = json.dumps({
            k: v for k, v in zip(
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "E.8.")]]/td[2]/text()').getall()],
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "E.8.")]]/td[3]/text()').getall()]
            )
        })
        item['trial_scope'] = json.dumps({
            k: v for k, v in zip(
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "E.6.")]]/td[2]/text()').getall()],
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "E.6.")]]/td[3]/text()').getall()]
            )
        })
        item['trial_phase'] = json.dumps({
            k: v for k, v in zip(
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "E.7.")]]/td[2]/text()').getall()],
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "E.7.")]]/td[3]/text()').getall()]
            )
        })
        item['start_date'] = response.xpath('//tr[td[2][normalize-space()="Date of Competent Authority Decision"]]/td[3]//text()').get().strip()
        item['end_date'] = response.xpath('//tr[td[2][normalize-space()="Date of the global end of the trial"]]/td[3]//text()').get()
        item['Protocol'] = response.xpath('//tr[td[1][normalize-space()="A.4.1"]]/td[3]//text()').get().strip()
        item['Sponsor'] = response.xpath('//tr[td[1][normalize-space()="B.1.1"]]/td[3]//text()').get().strip()
        item['therapeutic_area'] = response.xpath('//tr[td[1][normalize-space()="E.1.1.2"]]/td[3]//text()').get().strip()
        item['condition'] = [i.strip() for i in response.xpath('//tr[td[1][normalize-space()="E.1.1"]]/td[3]/table//td/text()').getall()]
        item['Disease'] = json.dumps({
            k: v for k, v in zip(
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "E.1.2")]]/td[2]/text()').getall()],
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "E.1.2")]]/td[3]/text()').getall()],
            )
        })
        item['Age'] = json.dumps({
            k: v for k, v in zip(
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "F.1.")]]/td[2]/text()').getall()],
                [i.strip() for i in response.xpath('//tr[td[1][contains(., "F.1.")]]/td[3]/text()').getall()]
            )
        })
        item['Gender'] = json.dumps({
            'F': response.xpath('//tr[td[1][normalize-space()="F.2.1"]]/td[3]/text()').get().strip(),
            'M': response.xpath('//tr[td[1][normalize-space()="F.2.2"]]/td[3]/text()').get().strip(),
        })
        item['inclusion_crt'] = json.dumps([i.strip() for i in response.xpath('//tr[td[1][normalize-space()="E.3"]]/td[3]/table//td/text()').getall()])
        item['exclusion_crt'] = json.dumps([i.strip() for i in response.xpath('//tr[td[1][normalize-space()="E.4"]]/td[3]/table//td/text()').getall()])
        item['status'] = response.xpath('//tr[td[2][normalize-space()="End of Trial Status"]]/td[3]/text()').get()
        item['url'] = response.url

        yield item
