import google.generativeai as genai

# ใส่ API Key ของคุณที่นี่
genai.configure(api_key="YOUR_API_KEY")

print("โมเดลที่รองรับการสร้างเนื้อหา (generateContent):")
for m in genai.list_models():
  if 'generateContent' in m.supported_generation_methods:
    print(m.name)

print("\nโมเดลสำหรับ Embedding:")
for m in genai.list_models():
    if 'embedContent' in m.supported_generation_methods:
        print(m.name)