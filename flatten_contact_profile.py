def flatten_contact_profile(contact_id_, contact_d):
  def process_entry(key_l_,out_l_, entry):
     if isinstance(entry, dict) and len(key_l_)>0:
        if key_l_[-1] == 'experience':
            curr_sentence = ''
            if 'position' in entry and entry['position'] is not None:
              curr_sentence+= entry['position']
      
              
            if 'job_description' in entry and entry['job_description'] is not None:    
                curr_sentence+=  ' and job description is ' + entry['job_description']    

            if curr_sentence != '':
               out_l_.append([key_l_, curr_sentence])
            return
   
   

        if key_l_[-1] == 'education':
            curr_sentence = ''
            if 'degree' in entry and len(entry['degree'])>0:
              curr_sentence+= ' in '.join(entry['degree'])

            if curr_sentence != '':
               out_l_.append([key_l_, curr_sentence])
            return
    

        if key_l_[-1] == 'projects':
            curr_sentence = ''
            if 'title' in entry and entry['title'] is not None:
              curr_sentence+= entry['title']

            if 'description' in entry and entry['description'] is not None:    
                curr_sentence+= ' and the description of project is ' + entry['description']

            if curr_sentence != '':
               out_l_.append([key_l_, curr_sentence])   
            return
    

            
     if isinstance(entry, str): 
            out_l_.append([key_l_, entry]) 
            return
     elif isinstance(entry, bool):
            out_l_.append([key_l_, str(entry)])
            return
     elif isinstance(entry, list):
            for i, item in enumerate(entry):
                process_entry(key_l_, out_l_, entry[i])
     elif isinstance(entry, dict):
            for k_, v_ in entry.items():
                process_entry(key_l_ + [k_.lower().replace(' ', '_')], out_l_, v_)
     else:
            return

  key_l = []
  out_l = []
  process_entry(key_l,out_l, contact_d)

  return out_l
