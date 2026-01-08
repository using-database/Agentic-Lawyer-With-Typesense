import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    client = MongoClient(os.getenv("MONGO_URI"))
    return client[os.getenv("DB_NAME")]


def cache_masters(db):
    states = {
        s["State_id"]: s["State"]
        for s in db.state_master.find()
    }

    doc_types = {
        dt["document_type_id"]: dt["document_type"]
        for dt in db.document_type_master.find()
    }

    laws = {
        str(l["Law_id"]): l["Law"]
        for l in db.law_master.find()
    }

    return states, doc_types, laws


def flatten_act_content(act_doc, law_map):
    flattened = []

    content = act_doc.get("Content", {})
    bodies = content.get("children", {}).get("akn-body", [])
    act_level_laws = act_doc.get("Law_ids", [])

    for body in bodies:
        sections = body.get("children", {}).get("akn-section", [])

        for sec in sections:
            sec_id = sec.get("section_id")
            sec_title = sec.get("title", "")

            subsections = sec.get("children", {}).get("akn-subsection", [])
            text_parts = []

            if subsections:
                for sub in subsections:
                    txt = sub.get("content", "").strip()
                    if txt:
                        text_parts.append(txt)
            else:
                txt = sec.get("content", "").strip()
                if txt:
                    text_parts.append(txt)

            full_text = " ".join(text_parts)

            section_laws = sec.get("Law_ids", [])

            if not section_laws:
                section_laws = [
                    l for l in act_level_laws
                    if l.get("section_id") == sec_id
                ]

            if not section_laws:
                section_laws = act_level_laws

            resolved_laws = {
                law_map.get(str(l.get("law_id")))
                for l in section_laws
                if law_map.get(str(l.get("law_id")))
            }

            flattened.append({
                "id": sec_id,
                "section_title": sec_title,
                "content": full_text,
                "act_title": act_doc.get("Title"),
                "doc_id": act_doc.get("Doc_id"),
                "year": act_doc.get("Year"),
                "law_names": list(resolved_laws) or ["General Law"]
            })

    return flattened


def main():
    db = get_db_connection()
    state_map, type_map, law_map = cache_masters(db)

    doc_id = input("Enter Doc_id: ").strip()
    act = db.acts_master.find_one({"Doc_id": doc_id})

    if not act:
        return

    flat_data = flatten_act_content(act, law_map)

    state_name = state_map.get(act.get("State"), "Unknown")
    doc_type = type_map.get(act.get("Type"), "Unknown")

    for item in flat_data:
        item["state_name"] = state_name
        item["doc_type"] = doc_type

    with open("./dataset/output.json", "w", encoding="utf-8") as f:
        json.dump(flat_data, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
