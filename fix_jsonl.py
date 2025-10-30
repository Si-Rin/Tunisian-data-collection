import json

def quick_fix_jsonl(input_file, output_file=None):
    """Quick fix for a single JSONL file"""
    if output_file is None:
        output_file = input_file.replace('.jsonl', '_fixed.jsonl')
    
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by objects (assuming each object starts with '{' and ends with '}')
    objects = []
    current_obj = []
    in_object = False
    brace_count = 0
    
    for line in content.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        if line.startswith('{') and not in_object:
            in_object = True
            current_obj = [line]
            brace_count = line.count('{') - line.count('}')
        elif in_object:
            current_obj.append(line)
            brace_count += line.count('{') - line.count('}')
            
            if brace_count == 0:
                obj_str = ''.join(current_obj)
                try:
                    json_obj = json.loads(obj_str)
                    objects.append(json_obj)
                except json.JSONDecodeError:
                    print(f"Failed to parse: {obj_str[:100]}...")
                current_obj = []
                in_object = False
    
    # Write fixed file
    with open(output_file, 'w', encoding='utf-8') as f:
        for obj in objects:
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')
    
    print(f"Fixed {len(objects)} objects to {output_file}")

# Usage

quick_fix_jsonl("collected_data/data_youtube_id_8wlN5coe_20251029_234025.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_9bmG_8-U_20251029_233917.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_bS4VpsEI_20251029_234041.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_c7bNxL5X_20251029_233926.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_cs_IfoSh_20251029_234030.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_e9Zu1sCx_20251029_233636.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_gd_YgVui_20251029_233900.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_J3ekaaxh_20251029_150316.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_nXDYorO5_20251029_233907.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_P9YsWNho_20251029_233849.jsonl")
quick_fix_jsonl("collected_data/data_youtube_id_QNbgDANI_20251029_234047.json")