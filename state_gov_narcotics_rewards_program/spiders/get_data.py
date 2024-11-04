import re

import scrapy
import pandas as pd
import unicodedata
from scrapy import Spider
from scrapy.cmdline import execute


class GetDataSpider(scrapy.Spider):
    name = "get_data"

    def __init__(self):
        super().__init__()
        self.data_list = []
        self.headers = None
        self.cookies = None

    def start_requests(self):
        self.cookies = {
            'nmstat': 'd3f915e5-78e5-5d8c-6591-1a6199e0e306',
            '_gcl_au': '1.1.1775556259.1729241394',
            '_hjSessionUser_1395777': 'eyJpZCI6ImE2ZTdlNDRiLWNmMmUtNTAxNi04ZjBiLTA2ZjViYzRlOTg0NCIsImNyZWF0ZWQiOjE3MjkyNDEzOTc5NzEsImV4aXN0aW5nIjp0cnVlfQ==',
            'aws-waf-token': '286c691d-0473-4dca-ace8-bcf9b42abd56:EQoAsu0YRycyAQAA:9qiZq+ZvwEGBKtOrpssFjRT/v4yuLyHxdLVZph1y+gJT1bvZLEB25nPSy+E/xydbNjDwlFU2mSDOa1nsBymR8FnfJxXfvUASwCjtFDrTAy4eTnWNUf7pVLdoEVNN40vEhORq9pT6z03HjdGkV1epzqQFEoUYtwEzaNuqR0N0h7LAUqrUHu3BzX7iE3i4nM/h6gPCOvXvHoksbfbVaxghXjqhLUiIVSFdzAqCaKhdwXWJWyzoSjs=',
            '_gid': 'GA1.2.1264052437.1730113427',
            '_hjSession_1395777': 'eyJpZCI6IjI3NDMxMThlLTNkNDktNDdlMS04ZWEzLTRiYzJjZDE2YmRkNSIsImMiOjE3MzAxMTM0Mjc3OTMsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=',
            '_ga_CSLL4ZEK4L': 'GS1.1.1730113426.3.1.1730114359.0.0.0',
            '_ga': 'GA1.2.153828738.1729241393',
            '_ga_N47R32EN4M': 'GS1.1.1730113426.3.1.1730114360.58.0.0',
            '_4c_': '%7B%22_4c_s_%22%3A%22lZBRb4MgFIX%2FSsOzmAuCqG%2FLliz7AcseGwu0krZikNV1jf99F9tuyZ42QxQ%2B7zk5nAuZOtuThqkCGJO8ZKVQGdnb80iaCwnOpM%2BJNGTDmNWl0lRUUFABraL1ltXUSmE04BGAkYx8LF4K8FUXIMScET3cPC5Ee2PRi9U5E7jodkRJ%2FERUFIDbIXjzruM6noc0N9nNajR7%2FGHsyWm7npyJXTLgFfzQzrpdFxErvtAhpJFc4n5yvfHTb92NfuukrJFugp9Gm7SPXfBHu2JLJo9NkLdFkdIGu7UhLGN4Gl1MQcfYRpvv%2FOmGsL87pVc6pA6XeAev20MSYe8ZeX5Yv748pWiyqHiliipnitdcMOyPzPdCgQOoEqQU2Fc8kKYqBaRnvlov%2FXL4y%2Fj1mvTYup7a%2Fl%2Fief4C%22%7D',
        }

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            # 'cookie': 'nmstat=d3f915e5-78e5-5d8c-6591-1a6199e0e306; _gcl_au=1.1.1775556259.1729241394; _ga_CSLL4ZEK4L=deleted; _ga_CSLL4ZEK4L=deleted; _hjSessionUser_1395777=eyJpZCI6ImE2ZTdlNDRiLWNmMmUtNTAxNi04ZjBiLTA2ZjViYzRlOTg0NCIsImNyZWF0ZWQiOjE3MjkyNDEzOTc5NzEsImV4aXN0aW5nIjp0cnVlfQ==; _ga_N47R32EN4M=deleted; _ga_N47R32EN4M=deleted; aws-waf-token=286c691d-0473-4dca-ace8-bcf9b42abd56:EQoAsu0YRycyAQAA:9qiZq+ZvwEGBKtOrpssFjRT/v4yuLyHxdLVZph1y+gJT1bvZLEB25nPSy+E/xydbNjDwlFU2mSDOa1nsBymR8FnfJxXfvUASwCjtFDrTAy4eTnWNUf7pVLdoEVNN40vEhORq9pT6z03HjdGkV1epzqQFEoUYtwEzaNuqR0N0h7LAUqrUHu3BzX7iE3i4nM/h6gPCOvXvHoksbfbVaxghXjqhLUiIVSFdzAqCaKhdwXWJWyzoSjs=; _gid=GA1.2.1264052437.1730113427; _hjSession_1395777=eyJpZCI6IjI3NDMxMThlLTNkNDktNDdlMS04ZWEzLTRiYzJjZDE2YmRkNSIsImMiOjE3MzAxMTM0Mjc3OTMsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; _ga_CSLL4ZEK4L=GS1.1.1730113426.3.1.1730114359.0.0.0; _ga=GA1.2.153828738.1729241393; _ga_N47R32EN4M=GS1.1.1730113426.3.1.1730114360.58.0.0; _4c_=%7B%22_4c_s_%22%3A%22lZBRb4MgFIX%2FSsOzmAuCqG%2FLliz7AcseGwu0krZikNV1jf99F9tuyZ42QxQ%2B7zk5nAuZOtuThqkCGJO8ZKVQGdnb80iaCwnOpM%2BJNGTDmNWl0lRUUFABraL1ltXUSmE04BGAkYx8LF4K8FUXIMScET3cPC5Ee2PRi9U5E7jodkRJ%2FERUFIDbIXjzruM6noc0N9nNajR7%2FGHsyWm7npyJXTLgFfzQzrpdFxErvtAhpJFc4n5yvfHTb92NfuukrJFugp9Gm7SPXfBHu2JLJo9NkLdFkdIGu7UhLGN4Gl1MQcfYRpvv%2FOmGsL87pVc6pA6XeAev20MSYe8ZeX5Yv748pWiyqHiliipnitdcMOyPzPdCgQOoEqQU2Fc8kKYqBaRnvlov%2FXL4y%2Fj1mvTYup7a%2Fl%2Fief4C%22%7D',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        }

        yield scrapy.Request(
            url="https://www.state.gov/inl-rewards-program/narcotics-rewards-program",
            headers=self.headers,
            cookies=self.cookies,
            callback=self.parse_records,
            dont_filter=True
        )

    def parse_records(self, response, **kwargs):
        total_records = response.xpath(
            '//div[@class="collection-info__total"]/span[@class="collection-info__number"]/text()').get()
        if total_records:
            yield scrapy.Request(
                url=f"https://www.state.gov/inl-rewards-program/narcotics-rewards-program/?results={total_records}&gotopage=&total_pages=1",
                headers=self.headers,
                cookies=self.cookies,
                callback=self.parse_links,
                dont_filter=True
            )

    def parse_links(self, response, **kwargs):
        links = response.xpath('//li[@class="collection-result"]/a[@class="collection-result__link"]/@href').getall()
        for link in links:
            yield scrapy.Request(
                # url='https://www.state.gov/seuxis-pausias-hernandez-solarte/',
                url=link,
                headers=self.headers,
                cookies=self.cookies,
                callback=self.parse_data,
                dont_filter=True
            )

    def parse_data(self, response, **kwargs):
        data_dict = {}
        data_dict['url'] = response.url
        data_dict['title'] = response.xpath('//h1[contains(@class,"featured-content__headline")]/text()').get().strip()
        data_dict['publish_date'] = response.xpath(
            '//div[@class="article-meta"]/p[@class="article-meta__publish-date"]/text()').get()
        data_dict['photo_url'] = response.xpath('//div[@class="entry-content"]/figure/img/@src').get()
        tag_data = self.extract_tags(response)
        data_dict.update(tag_data)
        main_content = response.xpath('//div[@class="entry-content"]/p')
        article = ''
        for data in main_content:
            external_links = data.xpath('.//a[@class="external-link__pdf"]/@href').getall()
            if external_links:
                data_dict['external_links'] = ' | '.join(external_links)
                continue
            content = data.xpath('.//text()').getall()
            content_string = ''.join(content)
            pattern = r'(?P<KEY>[A-Z\s,]+):\s*(?P<VALUE>.*?)(?=\n[A-Z\s,]+:|$)'

            matches = re.finditer(pattern, content_string)
            result = {}
            str_ = ''
            for match in matches:
                key = match.group("KEY").strip()
                value = match.group("VALUE").strip()

                # Check if there are any subsequent key-value pairs in the value
                subsequent_keys = re.findall(r'(?P<NEW_KEY>[A-Z\s,]+):\s*(?P<NEW_VALUE>[^:]+)', value)

                # If there are subsequent keys, process them
                if subsequent_keys:
                    # Store the current key and value
                    result[key] = value.split(" ".join(subsequent_keys[0]))[
                        0].strip()  # The main value before any subsequent key
                    for new_key, new_value in subsequent_keys:
                        result[new_key.strip()] = new_value.strip()
                        str_ = f"{new_key.strip()}: {new_value.strip()}"
                else:
                    result[key] = value

            for key, value in result.items():
                result[key] = value.replace(' ', ' ').replace(str_, '').strip()
                if value == 'Unknown':
                    result[key] = 'N/A'

            if not result:
                article += content_string
                continue
            data_dict.update(result)

        data_dict['article'] = article.strip()
        self.data_list.append(data_dict)

    def extract_tags(self, response):
        output_dict = {}
        tag_data = response.xpath('//div[@class="related-tags"]/div[@class="related-tags__pills"]/a')
        tags = tag_data.xpath('./text()').getall()
        tag_links = tag_data.xpath('./@href').getall()
        output_dict['tags'] = ' | '.join(tags)
        output_dict['tag_links'] = ' | '.join(tag_links)
        return output_dict

    def convert_to_date_format(self, df):
        # Remove any extraneous characters in the 'publish_date' and 'dob' columns
        df['publish_date'] = df['publish_date'].str.extract(r'(\w+ \d{1,2}, \d{4})', expand=False)
        df['dob'] = df['dob'].str.extract(r'(\w+ \d{1,2}, \d{4})', expand=False)

        df['publish_date'] = pd.to_datetime(df['publish_date']).dt.strftime('%Y-%m-%d')
        df['dob'] = pd.to_datetime(df['dob']).dt.strftime('%Y-%m-%d')
        return df

    def remove_diacritics(self, df, column_names):
        # Apply diacritic removal to each specified column
        for column_name in column_names:
            df[column_name] = df[column_name].apply(
                lambda x: ''.join(
                    char for char in unicodedata.normalize('NFD', x) if unicodedata.category(char) != 'Mn'
                ) if isinstance(x, str) else x  # Ensure it only applies to strings
            )
        return df

    def format_aliases(self, df):
        # Use regex to handle different types of quotation marks and commas
        df['aliases'] = df['aliases'].str.replace(r'[“”]', '', regex=True).str.replace(r'[，,]', ' | ',
                                                                                       regex=True).str.replace(
            r'\b(and|y|&)\b', ' | ', regex=True)

        # Replace 'None' or empty strings with 'N/A'
        df['aliases'] = df['aliases'].replace(['None', ''], 'N/A')

        # Strip any leading or trailing whitespace
        df['aliases'] = df['aliases'].str.strip()
        return df

    def remove_duplicate_columns(self, df):
        df['scars_tattoos_or_marks'] = df['scars_tatoos_or_marks'].combine_first(df['scars_tattoos_or_marks'])
        df['scars_tattoos_or_marks'] = df['marks'].combine_first(df['scars_tattoos_or_marks'])
        df = df.drop(columns=['scars_tatoos_or_marks', 'marks'])
        return df

    def close(self, spider: Spider, reason: str):
        df = pd.DataFrame(self.data_list)
        df.columns = (
            df.columns
            .str.replace(' ', '_')
            .str.replace('.', '')
            .str.replace(',', '')
            .str.replace('/', '_')
            .str.replace('-', '_')
            .str.replace('\s+', ' ', regex=True)
            .str.strip()
            .str.lower()
        )
        df = self.convert_to_date_format(df)
        df = self.remove_diacritics(df, df.columns)
        df = self.remove_duplicate_columns(df)
        df = self.format_aliases(df)
        df.fillna('N/A', inplace=True)
        df.to_excel("../files/state_gov_narcotics_rewards_program.xlsx", index=False)


if __name__ == '__main__':
    execute(f'scrapy crawl {GetDataSpider.name}'.split())
