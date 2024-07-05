import json
from read_json_file import read_json_file
from flatten_contact_profile import flatten_contact_profile 
import re  
import copy
import pandas as pd




# select relevant entries given in include_list and remove_list from the extracted file

def combine_entries_for_same_key(out_l_, final_out_d, exclude_duplicate_entries=True):
    #  form the final list key and entry [key~entry]. There can be many entries with the same key
    # combine all entries for the same key in a single entry
    out_l_ = list(set(out_l_))
    out_d_ = {}
    #  combine entries for the same key as "experience position" into one entry
    for e in out_l_:
        kv = e.split('~')
        kv_l = out_d_.get(kv[0], None)
        if kv_l is None:
            kv_l = []
            out_d_[kv[0]] = kv_l
        kv_l.append(kv[1])

    for fk, vl in out_d_.items():
        if exclude_duplicate_entries:
            ext_vl = list(set(vl))
        else:
            ext_vl = vl
        final_out_d[fk] = ' . '.join(ext_vl)








class XverumDataExtractor(object):
    def __init__(self, ref_data_dir,
                 exclude_clodura_introduced_keys=False,
                 join_entries=True,
                 exclude_duplicate_entries=True):

        self.include_keys_d = read_json_file(ref_data_dir + 'key_components_of_included_keys.json')
        self.join_entries = join_entries
        self.exclude_duplicate_entries = exclude_duplicate_entries

    def __call__(self, xverum_d_for_contact, id_key='linkedin_profile'):
 
        out_l = flatten_contact_profile(xverum_d_for_contact[id_key], xverum_d_for_contact)
        extracted_d = self.select_relevant_entries(out_l)
        return xverum_d_for_contact[id_key], extracted_d[0], extracted_d[1]


    
    def select_relevant_entries(self, entries_l):
        out_l = []
        included_entires = []
        for entry in entries_l:  # format  given in out_l in calling function
            key_l, v_ = entry  # key is of the format [key_component_1, key_component_2...]
            included_entry_l = []
            final_key_l = []
            for k_ in key_l:
                final_key_l.append(k_)
            final_key_l = ' '.join(final_key_l)
            if final_key_l in self.include_keys_d:
                included_entires.append(final_key_l)
                if self.join_entries:
                    included_entry_l.append(final_key_l + '~' + entry[1].replace('^', ',').replace(':', ' '))
                else:
                    included_entry_l.append(entry)

                if included_entry_l:
                   out_l.append(included_entry_l[0])

        if self.join_entries:
            final_out_d = {}  # output one entry for similar keys
            combine_entries_for_same_key(out_l, final_out_d)
        else:
            final_out_d = {}  # output each key individually and the data
            for e in out_l:
                final_out_d['^'.join(e[0])] = e[1]

        return final_out_d, included_entires








# Clean job description from Indeed Jobs
class TextCleaner(object):
    def __init__(self):
        self.punctuations_table_ = list('!"$%\\\'(),:;<=>?@[]^_`{|}~/*')  # removed . * / +
        self.punctuations_trans_table_ = {ord(k): ' ' for k in self.punctuations_table_}
        # remove urls'
        self.remove_url_ = re.compile(
            '(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?')
        # remove newline
        self.remove_newline_ = re.compile(r'\n')
        # replace space.space
        self.replace_dot_ = re.compile(r' +\. +')
        # replace multiple . with a single .
        self.replace_multiple_dot_ = re.compile(r'\.+')
        # replace multiple whitespaces with a single whitespace
        self.replace_multiple_ws_ = re.compile(r'\s+')
        # remove newline
        self.remove_newline_ = re.compile(r'\n')

        # match left and right single quotes
        self.single_quote_expr = re.compile(r'[\u2018\u2019]', re.U)
        # match all non-basic latin unicode
        self.unicode_chars_expr = re.compile(r'[\u0080-\uffff]', re.U)

    def cleanse_unicode(self, s):
        if not s:
            return ""

        temp = self.single_quote_expr.sub("'", s, re.U)
        temp = self.unicode_chars_expr.sub("", temp, re.U)
        return temp

    def __call__(self, text, clean_text=True):
        text = copy.copy(text.lower())
        # remove urls
        text = self.remove_url_.sub(' ', text)
        text = self.remove_newline_.sub('', text)
        # replace newlines with space       ?
        text = text.replace('\\n', ' ')     # ?
        # replace ** or * used as bullet point highlight to a .
        text = text.replace('*+', '.')
        # remove punctuations other than ., -  etc
        text = text.translate(self.punctuations_trans_table_)
        # replace multiple space.space with single.
        text = self.replace_dot_.sub('.', text)
        # replace multiple . with a single .
        text = self.replace_multiple_dot_.sub('.', text)
        # replace multiple whitespaces with a single whitespace
        text = self.replace_multiple_ws_.sub(' ', text)
        text = self.cleanse_unicode(text)

        return text









text_cleaner = TextCleaner()





def clean_and_concatenate_fields_in_dataframe(df, label_field, clean_text=True, include_field_name_in_text=False, insert_newline_after_every_key_field=True):
    text = []
    labels = [] 
    ids = []
    text_df = df.drop(columns=[label_field])
    
    for index, row in df.iterrows(): 
        concatenated_text = ""
        for field in text_df.columns:
             if field == 'linkedin_id': 
                 continue 
             if pd.notna(row[field]):
                ext_text = str(row[field])  # Ensure ext_text is a string
                if clean_text:
                    ext_text = text_cleaner(ext_text)
                if include_field_name_in_text:
                    ext_text = field.lower() + ': ' + ext_text
                if insert_newline_after_every_key_field:
                    ext_text = '\n' + ext_text
                concatenated_text += ext_text
        ids.append(row['linkedin_id'])
        text.append(concatenated_text)
        labels.append(row[label_field].split('.'))
        
    return ids, text, labels









def clean_and_concatenate_fields_in_dataframe_non_clodura_extracted(df, label_field, clean_text=True, include_field_name_in_text=False, insert_newline_after_every_key_field=True):
    text = []
    ids = []
    
    for index, row in df.iterrows(): 
        concatenated_text = ""
        for field in df.columns:
             if field == 'linkedin_id':
                 continue
             if pd.notna(row[field]):
                ext_text = str(row[field])  # Ensure ext_text is a string
                if clean_text:
                    ext_text = text_cleaner(ext_text)
                if include_field_name_in_text:
                    ext_text = field.lower() + ': ' + ext_text
                if insert_newline_after_every_key_field:
                    ext_text = '\n' + ext_text
                concatenated_text += ext_text
        text.append(concatenated_text)
        ids.append(row['linkedin_id']) 
    return ids, text








map_dict = {'operations':'operations', 'finance':'finance', 'purchase':'finance', 'engineering':'engineering', 'risk':'risk', 'sales':'sales', 'security':'engineering', 'it':'it', 'digital':'engineering', 'compliance':'compliance', 'marketing':'marketing', 'product management':'engineering', 'infrastructure':'engineering', 'data engineering':'engineering', 'corporate finance':'finance',  'solutions':'engineering', 'network security':'engineering', 'distribution':'operations', 'analytics':'engineering', 'research':'engineering', 'automation':'engineering', 'cyber security':'engineering', 'applications': 'engineering', 'data':'engineering', 'banking':'finance', 'artificial intelligence':'engineering',  'support':'sales', 'testing':'engineering',  'hr': 'hr',  'cloud':'engineering', 'training':'hr',  'fraud':'fraud', 'hiring':'hr', 'devops': 'engineering', 'tax':'finance',  'accounts':'finance',  'admin': 'admin',  'controller':'finance',   'customer service':'sales', 'account management':'sales', 'branding':'marketing',  'legal':'legal', 'production':'production',  'accounts payable': 'finance', 'social media':'marketing', 'media':'marketing', 'accounts receivable': 'finance',  'blockchain':'engineering',  'learning':'hr', 'payroll':'finance' ,  'iot':'engineering',  'pre sales':'sales',  'inside sales': 'sales',  'digital marketing':'marketing', 'manufacturing': 'manufacturing', 'product security':'engineering','product managemet':'engineering'} 










def get_text_and_labels(xverum_file_path,join_entries=True,exclude_duplicate_entries=True):
    data = json.load(xverum_file_path)
    ref_dir = '/Users/anushkasingh/Desktop/data preparation for i million dataset 2/data_preparation_pipeline_22_may_2024/ref_data/'
    xvde = XverumDataExtractor(ref_dir,
                               exclude_clodura_introduced_keys=False,
                               join_entries=True,
                               exclude_duplicate_entries=True)
    results = []
    linkedin_ids = []
    # Iterate over the data
    for d in data:
      contact_id, ext_d, new_data = xvde(d)
      results.append(ext_d)  # Collect the dictionary
      linkedin_ids.append(contact_id)

    # Create a DataFrame from the collected results
    df = pd.DataFrame(results)
    df['linkedin_id'] = linkedin_ids
    filtered_df = df[df['clodura_extracted functional_level'].notna()]
    desired_order = [
  "linkedin_id",
  "clodura_extracted functional_level",
  "designation",
  "xverum_json position",
  "xverum_json working_for type",
  "xverum_json experience",
  "xverum_json about_me",
  "xverum_json projects",
  "xverum_json education",
  "xverum_json courses"        
   ] 
    filtered_df = filtered_df[desired_order] 
    IDS,T,L = clean_and_concatenate_fields_in_dataframe(filtered_df,'clodura_extracted functional_level')

    L_modified = []
    for l in L:
       this_labels = []
       for label in l:
        this_labels.append(map_dict[(label.lower()).strip()])
       L_modified.append(list(set(this_labels)))

    data = {
      "linkedin_id": IDS,
    "text": T,
    "label": L_modified
    }

    # Create a DataFrame from the dictionary
    df = pd.DataFrame(data)
    return filtered_df, df














def get_text_and_labels_non_clodura_extracted(xverum_file_path,join_entries=True,exclude_duplicate_entries=True):
    data = json.load(xverum_file_path)
    ref_dir = '/Users/anushkasingh/Desktop/data preparation for i million dataset 2/data_preparation_pipeline_22_may_2024/ref_data/'
    xvde = XverumDataExtractor(ref_dir, 
                               exclude_clodura_introduced_keys=False,
                               join_entries=True,
         
                               exclude_duplicate_entries=True)
    
    linkedin_ids = []
    results = []
    # Iterate over the data
    for d in data:
      contact_id, ext_d, new_data = xvde(d)
      results.append(ext_d)  # Collect the dictionary
      linkedin_ids.append(contact_id)
    # Create a DataFrame from the collected results
    df = pd.DataFrame(results)
    df['linkedin_id'] = linkedin_ids
    filtered_df = df[df['clodura_extracted functional_level'].isna()]
    
    desired_order = [
  "linkedin_id",
  "designation",
  "xverum_json position",
  "xverum_json working_for type",
  "xverum_json experience",
  "xverum_json about_me",
  "xverum_json projects",
  "xverum_json education",
  "xverum_json courses"        
   ] 

    filtered_df = filtered_df[desired_order] 
    IDS, T = clean_and_concatenate_fields_in_dataframe_non_clodura_extracted(filtered_df,'clodura_extracted functional_level')
    data = {
    "linkedin_id": IDS,
    "text": T
    }

    # Create a DataFrame from the dictionary
    df = pd.DataFrame(data)  
    return filtered_df, df































































