# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col


session = get_active_session()
my_dataframe = session.table("SMOOTHIES.PUBLIC.ORDERS").filter(col("ORDER_FILLED")==0).collect()


if my_dataframe:
    #st.write(my_dataframe)
    editable_df = st.data_editor(my_dataframe)
    
    #st.write(editable_df)
    
    
    
    
    submitted = st.button("Submin")
    if submitted:
    
        try:
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)
            og_dataset.merge(edited_dataset
                             , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                             , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                            )
            st.success("Done!")
        
        except:    
            st.success("Something went wrong")

else:
    st.write("there is no data in your table")




























