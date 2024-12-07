import pandas as pd
import io
from google.colab import files
import aiohttp
import asyncio
from tqdm.notebook import tqdm
import nest_asyncio
from bs4 import BeautifulSoup
import re

nest_asyncio.apply()

print("Please upload your LinkedIn connections CSV file.")
uploaded = files.upload()
file_name = list(uploaded.keys())[0]

df = pd.read_csv(io.BytesIO(uploaded[file_name]), 
                 skiprows=4,
                 usecols=[0, 1, 2, 4, 5],
                 names=['First Name', 'Last Name', 'LinkedIn Profile', 'Company', 'Position'],
                 encoding='utf-8', 
                 on_bad_lines='skip')

print("First few rows of the dataframe:")
print(df.head())

high_positions = [
    'executive', 'director', 'president', 'ceo', 'cto', 'cio', 'vp', 'head',
    'chief', 'founder', 'co-founder', 'owner', 'partner', 'principal',
    'managing director', 'senior manager', 'lead', 'architect'
]

async def is_software_company(session, company_name):
    try:
        # Try Wikipedia first
        wiki_url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{company_name.replace(' ', '_')}"
        async with session.get(wiki_url) as response:
            if response.status == 200:
                data = await response.json()
                summary = data.get('extract', '').lower()
                
                software_indicators = [
                    "software company", "technology company", "tech company",
                    "software development", "cloud computing", "saas",
                    "internet company", "digital services", "it services",
                    "cybersecurity", "artificial intelligence", "machine learning",
                    "big data", "blockchain", "iot", "internet of things"
                ]
                
                if any(indicator in summary for indicator in software_indicators):
                    return True
                
                if re.search(r'\b(software|app|platform|cloud service|web service|api|sdk)\b', summary):
                    return True
        
        # If Wikipedia doesn't provide clear information, use Google search
        search_url = f"https://www.google.com/search?q={company_name} company type industry"
        async with session.get(search_url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text().lower()
            
            software_indicators = [
                "software company", "technology company", "tech company",
                "software development", "cloud computing", "saas provider",
                "internet company", "digital services company", "it services",
                "cybersecurity firm", "ai company", "machine learning company",
                "big data analytics", "blockchain technology", "iot solutions"
            ]
            
            for indicator in software_indicators:
                if indicator in text[:1500]:  # Expanded search area
                    return True
            
            # Check for software products or services
            product_indicators = [
                "develops software", "creates apps", "provides cloud services",
                "offers saas solutions", "builds digital platforms"
            ]
            
            for indicator in product_indicators:
                if indicator in text[:1500]:
                    return True
        
        return False
    except Exception as e:
        print(f"Error checking company {company_name}: {e}")
        return False

async def process_companies(companies):
    async with aiohttp.ClientSession() as session:
        tasks = [is_software_company(session, company) for company in companies]
        return await asyncio.gather(*tasks)

def filter_connections(row, software_companies):
    high_position = any(title.lower() in str(row['Position']).lower() for title in high_positions)
    software_company = software_companies.get(row['Company'], False)
    return high_position, software_company

async def main():
    unique_companies = df['Company'].unique()
    software_companies = dict(zip(unique_companies, await process_companies(unique_companies)))
    
    tqdm.pandas(desc="Processing rows")
    df[['High_Position', 'Software_Company']] = df.progress_apply(
        lambda row: filter_connections(row, software_companies), axis=1, result_type='expand'
    )

    df['Reason'] = df.apply(
        lambda row: 'High Position' if row['High_Position'] else ('Software Company' if row['Software_Company'] else 'Not Selected'), axis=1
    )

    filtered_df = df[df['Reason'] != 'Not Selected']
    output_file = 'filtered_connections.csv'
    filtered_df.to_csv(output_file, index=False)
    files.download(output_file)
    print("Processing complete. The filtered CSV file has been created and is ready for download.")

await main()
