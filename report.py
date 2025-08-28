def process_files(validation_errors, all_locations, start_date, end_date, total_locations,
                  progress_bar, status_text, select_categories):

    import streamlit as st
    import os
    import io
    import zipfile
    import pandas as pd
    from datetime import datetime, timedelta
    from collections import defaultdict

    #L_master = pd.read_csv(r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRtTfch6bBGz68DInLsqnFpO0jaGoJ5_etyz9zrG1wRK4NiIgZSg-5A85GBe1EVE9NZ8VfePfYndXmK/pub?gid=513644184&single=true&output=csv')
    L_master = pd.read_csv(r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRpk2X7zJhqiXMBU5tnmhvCkaqKCUXFifM5xFEFlHRwqTsx4klELI84EjKp3OWRa14X6AwJgjePPvhf/pub?gid=690667440&single=true&output=csv')
    # ---------- helpers ----------
    files = {}     # name -> bytes
    previews = {}  # name -> DataFrame

    def _store_xlsx(name: str, df: pd.DataFrame):
        previews[name] = df.copy()
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        files[name] = buf.getvalue()

    def read_file(file_path):
        try:
            if file_path.lower().endswith(('.xls', '.xlsx')):
                try:
                    if file_path.lower().endswith('.xlsx'):
                        return pd.read_excel(file_path, engine='openpyxl')
                    else:
                        try:
                            return pd.read_excel(file_path, engine='xlrd')
                        except:
                            try:
                                return pd.read_excel(file_path, engine='openpyxl')
                            except:
                                try:
                                    return pd.read_excel(file_path, engine='pyxlsb')
                                except:
                                    return try_read_as_csv(file_path)
                except Exception as e:
                    print(f"Excel read failed for {file_path}, trying CSV approach: {e}")
                    return try_read_as_csv(file_path)
            else:
                return try_read_as_csv(file_path)
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")
            return None

    def try_read_as_csv(file_path):
        try:
            return pd.read_csv(file_path,encoding='utf-8', sep=None, engine='python', on_bad_lines='skip')
        except UnicodeDecodeError:
            try:
                return pd.read_csv(file_path,encoding='windows-1252', sep=None, engine='python', on_bad_lines='skip')
            except Exception as e:
                print(f"CSV read failed for {file_path}: {e}")
                return None
   
    def to_num(s):
        return pd.to_numeric(s, errors="coerce").fillna(0)

     # ---------- per location ----------
    for i, (brand, dealer, Location, location_path) in enumerate(all_locations):
        progress_bar.progress((i + 1) / max(total_locations, 1))
        status_text.text(f"Generating reports for {Location} ({i+1}/{total_locations})...")
        
        mrn = []
        stock =[]
        po =[]

        for file in  os.listdir(location_path):
            file_path = os.path.join(location_path, file)
            if not os.path.isfile(file_path):
                continue

            fl = file.lower().strip()

            if fl.startswith('mrn'):
                df = read_file(file_path)
                df['Brand']=brand
                df['Dealer']=dealer
                df['Location']=Location
                df['_Sourcefile_']=file
                mrn.append(df)
            elif fl.lower().startswith('stock'):
                print(file)
                sdf = pd.read_table(file_path,encoding='utf-16')
                df = pd.concat([sdf], ignore_index=True)
                df['Brand']=brand
                df['Dealer']=dealer
                df['Location']=Location
                df['_Sourcefile_']=file
                stock.append(df)
            elif fl.lower().startswith('po'):
                df = read_file(file_path)
                df['Brand']=brand
                df['Dealer']=dealer
                df['Location']=Location
                df['_Sourcefile_']=file
                po.append(df)
               
    if stock:
        Stock_df = pd.concat(stock,ignore_index=True)
        Stock_df['Quantity'] = pd.to_numeric(Stock_df['Quantity'].astype(str).str.replace(',', '', regex=False),errors='coerce')
        Stock_df['Location_code'] = Stock_df['Inventory Location Name'].astype(str).str.split('-').str[4]
        Stock_df =L_master.merge(Stock_df,left_on='Code',right_on='Location_code',how='inner')

        stk = Stock_df[(Stock_df['Location_code'].notnull())&(Stock_df['Availability']=='On Hand')&(Stock_df['Quantity']>0)][['Brand','Dealer','Location_y','Location_code','Code','Part Number','Quantity']]
        stk.rename(columns={'Quantity':'Qty','Part Number':'PartNumber'},inplace=True)
        #_store_xlsx("Stock_brand_dealer_loc.xlsx", stk)
       # _store_xlsx('stock_'+brand+'_'+dealer+'_'+Location+'.xlsx',index=False)  
        _store_xlsx('stock_'+brand+'_'+dealer+'_'+Location+'.xlsx', stk)
    if mrn:
        Mrn_df = pd.concat(mrn,ignore_index=True)
        Mrn_df = L_master.merge(Mrn_df,left_on='Code',right_on='Network Code',how='inner')
        mrn_D = Mrn_df[(Mrn_df['Code'].notnull())&(Mrn_df['MRNs Actual Received Qty']>0)&(Mrn_df['Supplier Name']=='HCIL')]
        mrn_D = mrn_D[['Brand','Dealer','Location_x','Part Number','Order Number','Order Date','MRNs Actual Received Qty']]
        mrn_D['OEMInvoiceNo']=''
        mrn_D['OEMInvoiceDate']=''
        mrn_D['OEMInvoiceQty']=''
        mrn_D['MRNNumber']=''
        mrn_D['MRNDate']=''
        mrn_D.rename(columns={'Location_x':'Location','Part Number':'PartNumber','Order Number':'OrderNumber','Order Date':'OrderDate','MRNs Actual Received Qty':'ReceiptQty'},inplace=True)
        #mrn_D.to_excel('Mrn_'+brand+'_'+dealer+'_'+Location+'.xlsx',index=False)    
        _store_xlsx('Mrn_' + brand + '_' + dealer + '_' + Location + '.xlsx', mrn_D)

    if po:
        po_df = pd.concat(po,ignore_index=True)
        Po_df = L_master.merge(po_df,left_on='Code',right_on='Network Code',how='inner')
        Po_df[(Po_df['Code'].notnull())&(Po_df['Order Status']=='Sent To HCIL')&(Po_df['Quantity Requested']>0)]
        Po_D = Po_df[(Po_df['Order Status']=='Sent To HCIL')&(Po_df['Quantity Requested']>0)][['Brand','Dealer','Location_x','Part Number','Order Number','Order Date','Quantity Requested']]
        Po_D.rename(columns={'Location_x':'Location','Part Number':'PartNumber','Order Number':'OrderNumber','Order Date':'OrderDate','Quantity Requested':'POQty'},inplace=True)
        Po_D['OEMInvoiceNo']=''
        Po_D['OEMInvoiceDate']=''
        Po_D['OEMInvoiceQty']=''
        Po_D.drop_duplicates(inplace=True)
        #Po_D.to_excel('Po_'+brand+'_'+dealer+'_'+Location+'.xlsx',index=False)     
        _store_xlsx('Po_' + brand + '_' + dealer + '_' + Location + '.xlsx', Po_D)    
    
    # ---------- UI ----------
    if validation_errors:
        st.warning("⚠ Validation issues found:")
        for error in validation_errors:
            st.write(f"- {error}")

    st.success("🎉 Reports generated successfully!")
    st.subheader("📥 Download Reports")

   

    report_types = {
    'OEM':   [k for k in files.keys() if k.startswith('OEM_')],
    'Stock': [k for k in files.keys() if k.lower().startswith('stock_')],
    'Mrn':   [k for k in files.keys() if k.startswith('Mrn_')],
    'PO':    [k for k in files.keys() if k.startswith('Po_')],}


    for report_type, names in report_types.items():
        if not names:
            continue
        with st.expander(f"📂 {report_type} Reports ({len(names)})", expanded=False):
            for name in names:
                st.markdown(f"### 📄 {name}")

                # Show preview if we have it
                df_preview = previews.get(name)
                if df_preview is not None and not df_preview.empty:
                    st.dataframe(df_preview.head(5))
                else:
                    st.info("No preview available.")

                # Download button (bytes)
                blob = files.get(name)
                if blob:
                    st.download_button(
                        label="⬇ Download Excel",
                        data=blob,
                        file_name=name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{name}",
                    )
                else:
                    st.warning("⚠ Download content missing for this file.")

   # ---------- Combined ZIP per (report_type, brand, dealer) using previews (DataFrames) ----------
    grouped_data = defaultdict(list)
    for file_name, df in previews.items():
        if df is None or df.empty:
            continue
        parts = file_name.replace(".xlsx", "").split("_")
        if len(parts) >= 4:
            rep, br, dlr = parts[0], parts[1], parts[2]
            loc_part = "_".join(parts[3:])
            if "Location" not in df.columns:
                df = df.copy()
                df["Location"] = loc_part
            grouped_data[(rep, br, dlr)].append(df)
        else:
            st.warning(f"❗ Invalid file name format: {file_name}")

    if grouped_data:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for (rep, br, dlr), df_list in grouped_data.items():
                combined_df = pd.concat(df_list, ignore_index=True)
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                    combined_df.to_excel(writer, sheet_name="Sheet1", index=False)
                output_filename = f"{rep}_{br}_{dlr}.xlsx"
                zipf.writestr(output_filename, excel_buffer.getvalue())
        filename = f"{brand}_Combined_Dealerwise_Reports.zip"
        st.download_button(
            label=f"📦 Download Combined Dealer Reports ZIP",
            data=zip_buffer.getvalue(),
            file_name=filename,
            mime="application/zip",
        )
    else:
        st.info("ℹ No reports available to download.")
        st.waring("Pls check Folder Structure")

   


