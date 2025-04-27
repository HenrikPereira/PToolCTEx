import pandas as pd
import scrapy
from scrapy_playwright.page import PageMethod

from ..items import PAPItem


class PapInfarmedSpider(scrapy.Spider):
    name = "pap_infarmed"
    allowed_domains = ["infarmed.pt"]
    start_urls = [
        "https://www.infarmed.pt/web/infarmed/avaliacao-terapeutica-e-economica/programa-de-acesso-precoce-a-medicamentos"
    ]

    def parse(self, response, **kwargs):
        yield scrapy.Request(
            response.url,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    # Aguarda que o <select> esteja disponível.
                    PageMethod("wait_for_selector", "tbody"),
                    # Adiciona uma nova opção ao <select> (ajuste o seletor se necessário)
                    PageMethod("evaluate",
                               '''
                               $('.dataTables_length').find('select').append('<option value=1000>Novo</option>');
                               $('.dataTables_length').find('select').val(1000).trigger('change');
                               '''
                               ),
                    # Aguarda que a tabela com os resultados seja renderizada.
                    PageMethod("wait_for_selector", "table.dataTable.no-footer")
                ],
            },
            callback=self.parse_page
        )

    def parse_page(self, response):
        self.logger.info("Página renderizada e interações realizadas com sucesso.")

        # Tenta extrair os cabeçalhos da tabela
        headers = response.xpath("//table/thead/tr/th/text()").getall()
        if not headers:
            headers = response.xpath("//table/tr[1]/th/text()").getall()
        if not headers:
            headers = response.xpath("//table/tr[1]/td/text()").getall()
        headers = [h.strip() for h in headers if h.strip()]
        fields = ['Nome', 'DCI', 'decisao', 'data_decisao', 'detalhes', 'n_doentes', 'cond_observ']

        # Mapeamento dos textos dos cabeçalhos para os campos do PAPItem
        header_to_field = {k: v for k, v in zip(headers, fields)}

        # Extrai as linhas da tabela (preferencialmente dentro do <tbody>)
        rows = response.xpath("//table/tbody/tr")
        if not rows:
            rows = response.xpath("//table/tr")

        for row in rows:
            # Extrai os valores das células e limpa os espaços
            cells = row.xpath(".//td/text()").getall()
            cells = [cell.strip() for cell in cells if cell.strip()]
            if cells:
                item = PAPItem()
                # Percorre os pares (cabeçalho, célula) e mapeia para os campos do item
                for header, cell in zip(headers, cells):
                    field = header_to_field.get(header)
                    if field:
                        item[field] = cell

                # Processar 'cond_observ'
                if 'PAP ativo' in item.get('cond_observ', ''):
                    item['PAP_act'] = True
                else:
                    item['PAP_act'] = False

                cond_observ = item.get('cond_observ', '')
                if 'com custos' in cond_observ:
                    item['c_custos'] = True
                elif 'sem custos' in cond_observ:
                    item['c_custos'] = False
                else:
                    item['c_custos'] = pd.NA

                decisao = item.get('decisao', '')
                if 'indefer' in decisao.lower():
                    item['deferimento'] = False
                elif 'defer' in decisao.lower():
                    item['deferimento'] = True
                else:
                    item['deferimento'] = pd.NA

                # Processar 'n_doentes'
                try:
                    if '/' in item['n_doentes']:
                        item['recurr_n_dtes'] = item['n_doentes'].split('/')[-1]
                        item['n_doentes'] = int(item['n_doentes'].split('/')[0])
                    else:
                        item['n_doentes'] = int(item['n_doentes'])
                except (ValueError, TypeError):
                    self.logger.warning(f"Erro ao processar 'n_doentes': {item.get('n_doentes', '')}")
                    item['n_doentes'] = pd.NA

                # Passar item processado
                yield item

