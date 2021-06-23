import xlrd
import xlwt
import time
import math
import numpy
import concurrent.futures
import pandas as pd
from google.cloud import translate
from tqdm import tqdm



client = translate.TranslationServiceClient()

def detect_language(text, project_id="peppy-freedom-317516"):
    """Detecting the language of a text string."""

    location = "global"

    parent = f"projects/{project_id}/locations/{location}"

    # Detail on supported types can be found here:
    # https://cloud.google.com/translate/docs/supported-formats
    return client.detect_language(
        content=text,
        parent=parent,
        mime_type="text/plain",  # mime types: text/plain, text/html
    ).languages[0]


def translate_text(text, project_id="peppy-freedom-317516"):
    """Translating Text."""

    location = "global"

    parent = f"projects/{project_id}/locations/{location}"

    # Detail on supported types can be found here:
    # https://cloud.google.com/translate/docs/supported-formats
    return client.translate_text(
        request={
            "parent": parent,
            "contents": [text],
            "mime_type": "text/plain",  # mime types: text/plain, text/html
            "source_language_code": "ko",
            "target_language_code": "en",
        }
    ).translations[0]
    
def process_cell(row, column, value):
	#print (translator.detect(value))
	response = detect_language(value)
	if (response.language_code == "ko" and response.confidence > 0.9):
		value = translate_text(value).translated_text
	return row, column, value

start = time.time()  
file_path = input("full path of the document to translate :")
#Writing to file
wb_w = xlwt.Workbook()
#Reading file
wb_r = pd.read_excel(file_path, sheet_name=None, header = None, index_col= None)
MAX_WORKERS = 20

#Going through each cell to translate and rewriting
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
	for name, sheet in wb_r.items():
		sheet1 = wb_w.add_sheet(name)
		bar_title = "'" + name + "' is being translated"
		for row_index in tqdm(range(sheet.shape[0]), desc=bar_title , position=0, leave=True):
			futures = []
			for col_index in range(sheet.shape[1]):
				value = sheet.iat[row_index, col_index]
				if type(value) == str:
					futures.append(executor.submit(process_cell, row=row_index, column=col_index, value=value))
				elif type(value) == float:
					if math.isnan(value):
						sheet1.write(row_index, col_index, "")
					else:
						sheet1.write(row_index, col_index, float(value))
				elif type(value) == numpy.int64 or type(value) == int:
					sheet1.write(row_index, col_index, int(value))
				else:
					sheet1.write(row_index, col_index, str(value))
			for future in concurrent.futures.as_completed(futures):
				row, column, value = future.result()
				sheet1.write(row, column, value)

translated_filename = file_path.split('.')[0] + "_translated.xlsx"
wb_w.save(translated_filename)
end = time.time()
print("took : " +  str(end - start) +  " second(s)")



