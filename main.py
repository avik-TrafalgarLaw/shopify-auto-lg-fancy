import pandas as pd
import numpy as np
import requests
from datetime import datetime
import os
from ftplib import FTP
from google.cloud import storage

##############################################
# PART 0: FTP DOWNLOAD: Retrieve CSV from remote FTP server
##############################################

ftp_server = "ftp.nivoda.net"
ftp_user = "leeladiamondscorporate@gmail.com"
ftp_password = "r[Eu;9NB"
remote_file = "Leela Diamond_labgrown.csv"
local_file = "Labgrown.csv"  # Relative path

try:
    with FTP(ftp_server) as ftp:
        ftp.login(user=ftp_user, passwd=ftp_password)
        print("FTP login successful.")
        with open(local_file, "wb") as f:
            ftp.retrbinary("RETR " + remote_file, f.write)
        print(f"Downloaded '{remote_file}' to '{local_file}'.")
except Exception as e:
    print("Error downloading file from FTP:", e)
    exit(1)

##############################################
# PART 1: DATA IMPORT, FILTERING & BALANCED SELECTION FOR FANCY CERTIFIED DIAMONDS
##############################################

def map_shape(row):
    raw_shape = str(row.get('shape', '')).strip().upper()
    try:
        float(row.get('length', 0))
        float(row.get('width', 0))
    except Exception:
        pass
    if raw_shape in ['SQ EMERALD', 'ASSCHER']:
        return 'Asscher'
    if raw_shape in ['CUSHION', 'CUSHION BRILLIANT']:
        return 'Cushion'
    allowed = ['ROUND', 'OVAL', 'PRINCESS', 'EMERALD', 'MARQUISE', 'PEAR', 'RADIANT', 'HEART']
    if raw_shape in allowed:
        return raw_shape.title()
    return None

def compute_ratio(row):
    try:
        l = float(row.get('length', 0))
        w = float(row.get('width', 0))
        if w:
            return l / w
    except Exception:
        pass
    return np.nan

def compute_measurement(row):
    return f"{row.get('length', '')} x {row.get('width', '')} - {row.get('height', '')}"

def valid_cut(row):
    shape = str(row.get('FinalShape', '')).upper()
    if shape == 'ROUND':
        cut = str(row.get('cut', '')).strip().upper()
        return cut in ['EX', 'IDEAL', 'EXCELLENT']
    else:
        return True

def clarity_group(clarity_raw):
    clarity_raw = str(clarity_raw).upper().strip()
    if clarity_raw.startswith("VVS"):
        return "VVS"
    elif clarity_raw.startswith("VS"):
        return "VS"
    else:
        return None

def clarity_matches(row_clarity, group_clarity):
    grp = clarity_group(row_clarity)
    if group_clarity == 'VS-VVS':
        return grp in ['VVS', 'VS']
    else:
        return grp == group_clarity

# Read CSV and normalize column names
df = pd.read_csv(local_file, sep=',', low_memory=False,
                 dtype={'floCol': str, 'canadamarkeligible': str})
df.columns = [col.strip().lower() for col in df.columns]
print("Normalized columns:", df.columns.tolist())

# For Fancy Color Diamonds:
# Exclude any record whose color value is exactly one letter.
df = df[df['col'].str.strip().str.len() > 1]
# Retain only diamonds certified by IGI or GIA using the 'lab' column.
df = df[df['lab'].isin(['IGI', 'GIA'])]
df = df[df['image'].notnull() & (df['image'].astype(str).str.strip() != "")]
df = df[df['video'].notnull() & (df['video'].astype(str).str.strip() != "")]

df['FinalShape'] = df.apply(map_shape, axis=1)
allowed_shapes = ['Round', 'Oval', 'Princess', 'Emerald', 'Asscher', 'Cushion', 'Marquise', 'Pear', 'Radiant', 'Heart']
df = df[df['FinalShape'].isin(allowed_shapes)]
df['Ratio'] = df.apply(compute_ratio, axis=1)
df['Measurement'] = df.apply(compute_measurement, axis=1)
df['v360 link'] = df['reportno'].apply(lambda x: f"https://loupe360.com/diamond/{x}/video/500/500")

# Apply quality filters.
df = df[df['pol'].astype(str).str.strip().str.upper().isin(['EX', 'EXCELLENT'])]
df = df[df['symm'].astype(str).str.strip().str.upper().isin(['EX', 'EXCELLENT'])]
df = df[df.apply(valid_cut, axis=1)]

# Define carat/clarity groups.
groups = [
    {'min_carat': 0.95, 'max_carat': 1.10, 'clarity': 'VVS', 'count': 28},
    {'min_carat': 0.95, 'max_carat': 1.10, 'clarity': 'VS', 'count': 20},
    {'min_carat': 1.45, 'max_carat': 1.60, 'clarity': 'VVS', 'count': 28},
    {'min_carat': 1.45, 'max_carat': 1.60, 'clarity': 'VS', 'count': 20},
    {'min_carat': 1.95, 'max_carat': 2.10, 'clarity': 'VVS', 'count': 28},
    {'min_carat': 1.95, 'max_carat': 2.10, 'clarity': 'VS', 'count': 20},
    {'min_carat': 2.45, 'max_carat': 2.60, 'clarity': 'VVS', 'count': 28},
    {'min_carat': 2.45, 'max_carat': 2.60, 'clarity': 'VS', 'count': 20},
    {'min_carat': 2.95, 'max_carat': 3.10, 'clarity': 'VVS', 'count': 28},
    {'min_carat': 2.95, 'max_carat': 3.10, 'clarity': 'VS', 'count': 20},
    {'min_carat': 3.45, 'max_carat': 3.60, 'clarity': 'VVS', 'count': 28},
    {'min_carat': 3.45, 'max_carat': 3.60, 'clarity': 'VS', 'count': 20},
    {'min_carat': 3.95, 'max_carat': 4.10, 'clarity': 'VVS', 'count': 28},
    {'min_carat': 3.95, 'max_carat': 4.10, 'clarity': 'VS', 'count': 20},
    {'min_carat': 4.50, 'max_carat': 4.99, 'clarity': 'VS-VVS', 'count': 28},
    {'min_carat': 5.00, 'max_carat': 5.99, 'clarity': 'VS-VVS', 'count': 28},
    {'min_carat': 6.00, 'max_carat': 6.99, 'clarity': 'VS-VVS', 'count': 28},
    {'min_carat': 7.00, 'max_carat': 7.99, 'clarity': 'VS-VVS', 'count': 28},
    {'min_carat': 8.00, 'max_carat': 8.99, 'clarity': 'VS-VVS', 'count': 28},
]

target_per_shape = 480
final_selection = []

for shape in allowed_shapes:
    shape_pool = df[df['FinalShape'] == shape]
    shape_selected = pd.DataFrame()
    for grp in groups:
        group_df = shape_pool[
            (shape_pool['carats'] >= grp['min_carat']) &
            (shape_pool['carats'] <= grp['max_carat']) &
            (shape_pool['clar'].apply(lambda x: clarity_matches(x, grp['clarity'])))
        ]
        group_df_sorted = group_df.sort_values(by='price', ascending=True)
        group_sel = group_df_sorted.head(grp['count'])
        shape_selected = pd.concat([shape_selected, group_sel])
    shape_selected = shape_selected.drop_duplicates()
    current_count = len(shape_selected)
    if current_count < target_per_shape:
        additional_candidates = shape_pool.drop(shape_selected.index, errors='ignore')
        additional_sorted = additional_candidates.sort_values(by='price', ascending=True)
        needed = target_per_shape - current_count
        additional_sel = additional_sorted.head(needed)
        shape_selected = pd.concat([shape_selected, additional_sel])
    if len(shape_selected) > target_per_shape:
        shape_selected = shape_selected.sort_values(by='price', ascending=True).head(target_per_shape)
    final_selection.append(shape_selected)

final_df = pd.concat(final_selection).reset_index(drop=True)
print(f"Balanced selection complete: {len(final_df)} fancy color diamonds selected.")

today_str = datetime.today().strftime("%Y%m%d")
final_df['stock id'] = final_df.index + 1
final_df['stock id'] = final_df['stock id'].apply(lambda x: f"NVL-{today_str}-{x:02d}")

# Rename columns – note we now map 'lab' (not 'labtest') to 'LAB'
final_df.rename(columns={
    'lab': 'LAB',
    'reportno': 'REPORT NO',
    'FinalShape': 'Shape',
    'carats': 'Carat',
    'col': 'Color',
    'clar': 'Clarity',
    'price': 'Price',
    'cut': 'Cut',
    'pol': 'Polish',
    'symm': 'Symmetry',
    'flo': 'Fluor'
}, inplace=True)

# Write the transformed fancy color diamonds file.
selected_output_filename = "transformed_fancy_diamonds.csv"
final_df.to_csv(selected_output_filename, index=False)
print(f"Selected fancy color diamonds file written with {len(final_df)} diamonds at {selected_output_filename}.")

##############################################
# PART 2: PRICE CONVERSION & SHOPIFY UPLOAD FORMAT TRANSFORMATION
##############################################

def get_usd_to_cad_rate():
    url = "https://v6.exchangerate-api.com/v6/20155ba28afe7c763416cc23/latest/USD"
    try:
        response = requests.get(url)
        data = response.json()
        return data["conversion_rates"]["CAD"]
    except Exception as e:
        print("Error fetching exchange rate:", e)
        return 1.0

usd_to_cad_rate = get_usd_to_cad_rate()
print(f"USD to CAD rate: {usd_to_cad_rate}")

def markup(x):
    cad = x * usd_to_cad_rate
    base = cad * 1.05 * 1.13
    additional = (
        210 if cad <= 500 else
        375 if cad <= 1000 else
        500 if cad <= 1500 else
        700 if cad <= 2000 else
        900 if cad <= 2500 else
        1100 if cad <= 3000 else
        1200 if cad <= 5000 else
        1500 if cad <= 100000 else
        0
    ) * 1.15
    return round(base + additional, 2)

final_df['CAD_Price'] = final_df['Price'].apply(markup).round(2)
final_df['Compare_At_Price'] = (final_df['CAD_Price'] * 1.5).round(2)
final_df['Ratio'] = final_df['Ratio'].round(2)

custom_collection = f"Lab-Created Fancy Diamonds-{today_str}"

def clean_image_url(url):
    if pd.isna(url):
        return url
    if "?" in url:
        return url.split("?")[0]
    return url

def generate_handle(row):
    return f"Lab-Grown-{row['Shape']}-Diamond-{row['Carat']}-Carat-{row['Color']}-{row['Clarity']}-Clarity-{row['REPORT NO']}"

def generate_title(row):
    return f"{row['Shape']}-{row['Carat']}-Carat-{row['Color']}-{row['Clarity']}-{row['LAB']}-Certified - {row['REPORT NO']}"

def generate_body_html(row):
    return (f"Discover timeless beauty with our {row['Shape']} Cut Diamond, a stunning {row['Carat']}-carat gem "
            f"boasting a unique {row['Color']} color and impeccable {row['Clarity']} clarity. Certified by {row['LAB']}. "
            f"Report Number: {row['REPORT NO']}. Elevate your jewelry collection with this exquisite combination of elegance and brilliance.")

def generate_tags(row):
    return f"Lab-Created Fancy Diamonds-{today_str}"

def generate_image_alt(row):
    return (f"Lab-Grown {row['Shape']} Diamond - {row['Carat']} Carats, {row['Color']} Color, {row['Clarity']} Clarity - "
            f"Certified by {row['LAB']} - Report Number: {row['REPORT NO']}")

def generate_title_tag(row):
    return (f"Lab-Grown {row['Shape']} Diamond, {row['Carat']} Carats, {row['Color']} Color, {row['Clarity']} Clarity, "
            f"{row['LAB']} Certified - Report Number: {row['REPORT NO']}")

def generate_viewcertilink(row):
    report_no = row["REPORT NO"]
    lab = row["LAB"].upper()
    if lab == "IGI":
        return f"https://www.igi.org/verify-your-report/?r={report_no}"
    elif lab == "GIA":
        return f"https://www.gia.edu/report-check?locale=en_US&reportno={report_no}"
    else:
        return ""

shopify_df = pd.DataFrame({
    "Handle": final_df.apply(generate_handle, axis=1),
    "Title": final_df.apply(generate_title, axis=1),
    "Body HTML": final_df.apply(generate_body_html, axis=1),
    "Tags": final_df.apply(generate_tags, axis=1),
    "Image Src": final_df["image"].apply(clean_image_url),
    "Image Alt Text": final_df.apply(generate_image_alt, axis=1),
    "Variant Price": final_df["CAD_Price"].apply(lambda x: f"${x:.2f}"),
    "Variant Compare At Price": final_df["Compare_At_Price"].apply(lambda x: f"${x:.2f}"),
    "Metafield: title_tag [string]": final_df.apply(generate_title_tag, axis=1),
    "Metafield: description_tag [string]": final_df.apply(generate_body_html, axis=1),
    "Metafield: custom.diacertilab [single_line_text_field]": final_df["LAB"],
    "Metafield: custom.diacertino [number_integer]": final_df["REPORT NO"],
    "Metafield: custom.shape [single_line_text_field]": final_df["Shape"],
    "Metafield: custom.diacarat [number_decimal]": final_df["Carat"],
    "Metafield: custom.diacolor [single_line_text_field]": final_df["Color"],
    "Metafield: custom.diaclarity [single_line_text_field]": final_df["Clarity"],
    "Metafield: custom.diacut [single_line_text_field]": final_df["Cut"],
    "Metafield: custom.diapolish [single_line_text_field]": final_df["Polish"],
    "Metafield: custom.diasymmetry [single_line_text_field]": final_df["Symmetry"],
    "Metafield: custom.diaflourence [single_line_text_field]": final_df["Fluor"],
    "Metafield: custom.360_video [url]": final_df["v360 link"],
    "Metafield: custom.viewcertilink [url]": final_df.apply(generate_viewcertilink, axis=1),
    "Metafield: custom.diameasurement [single_line_text_field]": final_df["Measurement"],
    "Metafield: custom.diaratio [number_decimal]": final_df["Ratio"].apply(lambda x: f"{x:.2f}"),
    "Custom Collections": custom_collection,
    "Metafield: shopify.jewelry-type [list.metaobject_reference]": "shopify--jewelry-type.fine-jewelry",
    "Metafield: shopify.target-gender [list.metaobject_reference]": "shopify--target-gender.unisex",
    "Metafield: shopify.jewelry-material [list.metaobject_reference]": "shopify--jewelry-material.diamond",
    "Metafield: shopify.color-pattern [list.metaobject_reference]": "shopify--color-pattern.gold, shopify--color-pattern.white, shopify--color-pattern.rose-gold",
    "Variant Metafield: mm-google-shopping.age_group [single_line_text_field]": "adult",
    "Variant Metafield: mm-google-shopping.gender [single_line_text_field]": "unisex",
    "Variant Metafield: mm-google-shopping.color [single_line_text_field]": "white/yellow/rose gold",
    "Metafield: msft_bingads.bing_product_category [string]": "Apparel & Accessories > Jewelry > Loose Stones > Diamonds",
    "Metafield: msft_bingads.age_group [string]": "adult",
    "Metafield: msft_bingads.gender [string]": "unisex",
    "Vendor": "Lab-Grown",
    "Type": "Lab-Grown Diamond",
    "Template Suffix": "lab_grown-diamond",
    "Category: ID": "331",
    "Category: Name": "Jewelry",
    "Category": "Apparel & Accessories > Jewelry",
    "Variant Taxable": "FALSE",
    "Included / Canada": "TRUE",
    "Included / International": "TRUE",
    "Included / United States": "TRUE"
})

shopify_output_filename = f"shopify-lg-fancy-{today_str}.csv"
shopify_df.to_csv(shopify_output_filename, index=False)
print(f"Shopify upload file created with {len(shopify_df)} fancy color diamonds at {shopify_output_filename}.")

##############################################
# PART 3: UPLOAD TO GOOGLE CLOUD STORAGE
##############################################

def upload_to_gcs(source_file, destination_blob, bucket_name):
    # Load credentials explicitly from the file at /tmp/gcp_credentials.json
    storage_client = storage.Client.from_service_account_json("/tmp/gcp_credentials.json")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"File {source_file} uploaded to {destination_blob} in bucket {bucket_name}.")

bucket_name = "sitemaps.leeladiamond.com"
destination_blob = f"shopify final/{shopify_output_filename}"
upload_to_gcs(shopify_output_filename, destination_blob, bucket_name)
