import sys
import os
import requests



import pytesseract
from PIL import Image

def extractText(lines):
	
	urlIndex = 0
	for url in lines:
		urlIndex += 1
		text = ''
		try:
			filename = url.rsplit('/', 1)[-1]
			img_data = requests.get(url).content
			with open(filename, 'wb') as handler:
				handler.write(img_data)

			# Simple image to string
			text = pytesseract.image_to_string(Image.open(filename))
		except IOError as e:
			print "line "+ `urlIndex` + "\n{0}".format(e.message) + "\n"
			#print text + "I/O error({0}): {1}".format(e.errno, e.strerror)
		except Exception as e:
			print "line "+ `urlIndex` + "\n{0}".format(e.message) + "\n"
			#print text + "Unexpected error:", sys.exc_info()[0]
		try:
			os.remove(filename)
		except OSError:
			pass

if __name__ == "__main__":
	filename = sys.argv[1]
	with open(filename) as f:
		content = f.read().splitlines()
	extractText(content)