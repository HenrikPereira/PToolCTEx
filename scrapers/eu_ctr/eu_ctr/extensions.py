import logging
import datetime
from scrapy import signals

class CustomStatsExtension:
    def __init__(self):
        # Regista o horário de início do scraping
        self.start_time = datetime.datetime.now()

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        # Conecta o spider_closed ao sinal de fecho do spider
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_closed(self, spider, reason):
        end_time = datetime.datetime.now()
        duration = end_time - self.start_time

        # Obtém as estatísticas acumuladas pelo Scrapy
        stats = spider.crawler.stats.get_stats()

        # Recupera o número de erros e itens obtidos
        errors = stats.get('log_count/ERROR', 0)
        items_scraped = stats.get('item_scraped_count', 0)

        # Formata a mensagem de log com as informações desejadas
        log_message = (
            f"Data do Scrape: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Duração: {duration}\n"
            f"Nº de Erros: {errors}\n"
            f"Itens Obtidos com Sucesso: {items_scraped}\n"
            f"Motivo do Fecho: {reason}"
        )

        # Regista a mensagem de log
        logging.info(log_message)
