import streamlit as st
import json
import pandas as pd
import new  
import  predict_on_bulk_data
from flatten_and_extract_features import get_text_and_labels, get_text_and_labels_non_clodura_extracted


# Function to get session state
def get_session_state():
    if 'session_state' not in st.session_state:
        st.session_state['session_state'] = {}
    return st.session_state['session_state']

# Function to store generated CSV data in session state
def store_csv_data(key, data):
    session_state = get_session_state()
    session_state[key] = data

# Function to retrieve stored CSV data from session state
def get_csv_data(key):
    session_state = get_session_state()
    if key in session_state:
        return session_state[key]
    return None

# Function to clear stored CSV data from session state
def clear_session_state():
    st.session_state.pop('session_state', None)

def functional_level_prediction(text):
    result = new.find_functional_label(text)
    return result
 
# Main Streamlit app code
def main():
    user_input = st.text_area('Enter Profile Description To Get Its Functional Level')
    button = st.button('Predict')
 
    if user_input and button:
        result = functional_level_prediction(user_input)
        st.write('Functional Level: ', result)

    st.write('OR')

    uploaded_file = st.file_uploader('Upload a .json file', type=["json"])
    upload_button = st.button('Predict On File')

    if uploaded_file and upload_button:
        # Clear session state to reset previously stored data and buttons
        clear_session_state()

        if uploaded_file.type == "application/json":
            # Read the JSON file directly from the uploaded file object
            uploaded_file.seek(0)
            data = json.load(uploaded_file)

                # Reset file pointer to the beginning after reading it
            uploaded_file.seek(0)

                # Process the uploaded file to get text and labels
            _, labelled_concatenated_data = get_text_and_labels(uploaded_file)
            uploaded_file.seek(0)
            _, non_labelled_concatenated_data = get_text_and_labels_non_clodura_extracted(uploaded_file)

                # Get predicted labels on labelled and non-labelled datasets
            predicted_labels_data_on_labelled_dataset,hamming_score,flat_score = predict_on_bulk_data.get_predicted_dataframe_and_hammingscore_and_flat_score(labelled_concatenated_data)
            predicted_labels_data_on_non_labelled_dataset = predict_on_bulk_data.get_predicted_dataframe(non_labelled_concatenated_data)

            st.write("hamming_score: "+ str(hamming_score)) 
            st.write("flat_score: "+ str(flat_score)) 
                # Check if any of the DataFrames are empty before proceeding
            if not (labelled_concatenated_data.empty and non_labelled_concatenated_data.empty
                        and predicted_labels_data_on_labelled_dataset.empty and predicted_labels_data_on_non_labelled_dataset.empty):

                if not labelled_concatenated_data.empty:
                        LABELLED_CONCATENATED_DATA = labelled_concatenated_data.to_csv(index=False).encode('utf-8')
                        store_csv_data('labelled', LABELLED_CONCATENATED_DATA)

                if not non_labelled_concatenated_data.empty:
                        NON_LABELLED_CONCATENATED_DATA = non_labelled_concatenated_data.to_csv(index=False).encode('utf-8')
                        store_csv_data('non_labelled', NON_LABELLED_CONCATENATED_DATA)

                if not predicted_labels_data_on_labelled_dataset.empty:
                        PREDICTED_LABELS_LABELLED_DATA = predicted_labels_data_on_labelled_dataset.to_csv(index=False).encode('utf-8')
                        store_csv_data('predicted_labelled', PREDICTED_LABELS_LABELLED_DATA)

                if not predicted_labels_data_on_non_labelled_dataset.empty:
                        PREDICTED_LABELS_NON_LABELLED_DATA = predicted_labels_data_on_non_labelled_dataset.to_csv(index=False).encode('utf-8')
                        store_csv_data('predicted_non_labelled', PREDICTED_LABELS_NON_LABELLED_DATA)

            else:
                    st.write("No data available to process.")


    # Display download buttons for stored CSV data
    for key in ['labelled', 'non_labelled', 'predicted_labelled', 'predicted_non_labelled']:
        data = get_csv_data(key)
        if data:
            st.download_button(
                label=f"Download {key.replace('_', ' ')} file",
                data=data,
                file_name=f"{key.upper()}.csv",
                mime='text/csv',
            )

# Run the main function
if __name__ == "__main__":
    main()
