import requests

def get_random_paper(api_entities:str = 'works'):
    try:
        url = f"https://api.openalex.org/{api_entities}/random"
        is_oa = False
        i = 1

        while is_oa == False:
            res = requests.get(url).json()
            is_oa = res['open_access']['is_oa']
            i+=1

        data = {}

        data['display_name'] = res.get('display_name')
        data['1st_author'] = res['authorships'][0]['author']['display_name']
        data['publication_date'] = res.get('publication_date')
        data['cited_by_count'] = res.get('cited_by_count', None)
        data['oa_url'] = res['open_access']['oa_url']
        data['type'] = res['type']
        data['domain'] = res['primary_topic']['domain']['display_name']
        data['subfield'] = res['primary_topic']['subfield']['display_name']

        print("random 논문 가져오기 성공")
        return data
    except Exception as e:
        print("random 논문 가져오기 실패")
        raise e
