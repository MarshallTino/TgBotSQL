import google.generativeai as genai
from dotenv import load_dotenv
import os

load_dotenv()

def gemini_classify(text, api_key=os.getenv("GEMINI_API_KEY")):
    if not api_key:
        return True
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-pro")
        prompt = f"Clasifica como RELEVANTE sólo si menciona nuevo token:\n'{text}'"
        resp = model.generate_content(prompt)
        return "relevante" in resp.text.strip().lower()
    except Exception as e:
        print(f"⚠️ Error en gemini_classify: {e}. Fallback => True")
        return True
