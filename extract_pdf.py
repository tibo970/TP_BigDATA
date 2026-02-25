from pypdf import PdfReader

reader = PdfReader("Kafka spark.pdf")
text = ""
for page in reader.pages:
    text += page.extract_text() + "\n"

with open("pdf_text.txt", "w", encoding="utf-8") as f:
    f.write(text)
    
print("Successfully extracted PDF to pdf_text.txt")
