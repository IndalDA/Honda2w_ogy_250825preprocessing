def process_files(validation_errors, all_locations, start_date, end_date, total_locations,
                  progress_bar, status_text, select_categories):

    import streamlit as st
    import os, io, zipfile
    import pandas as pd
    import re
    from collections import defaultdict
    from datetime import datetime
    import urllib.error

    # ---------- fetch master ----------
    try:
        L_master = pd.read_csv(
            r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRpk2X7zJhqiXMBU5tnmhvCkaqKCUXFifM5xFEFlHRwqTsx4klELI84EjKp3OWRa14X6AwJgjePPvhf/pub?gid=690667440&single=true&output=csv'
        )
    except urllib.error.URLError:
        st.warning("⚠ Unable to fetch master data from Google Sheets. Please check your internet connection.")
        L_master = pd.DataFrame()

    # ---------- storages ----------
    file_bytes = {}   # name -> bytes   (renamed from 'files' to avoid shadowing)
    previews   = {}   # name -> DataFrame

    def _store_xlsx(name: str, df: pd.DataFrame):
        previews[name] = df.copy()
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        file_bytes[name] = buf.getvalue()

    def read_file(file_path):
        try:
            if file_path.lower().endswith('.xlsx'):
                return pd.read_excel(file_path, engine='openpyxl')
            elif file_path.lower().endswith('.xls'):
                for eng in ('xlrd', 'openpyxl', 'pyxlsb'):
                    try:
                        return pd.read_excel(file_path, engine=eng)
                    except Exception:
                        pass
                return try_read_as_csv(file_path)
            else:
                return try_read_as_csv(file_path)
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")
            return None

    def try_read_as_csv(file_path):
        try:
            return pd.read_csv(file_path, encoding='utf-8', sep=None, engine='python', on_bad_lines='skip')
        except UnicodeDecodeError:
            try:
                return pd.read_csv(file_path, encoding='windows-1252', sep=None, engine='python', on_bad_lines='skip')
            except Exception as e:
                print(f"CSV read failed for {file_path}: {e}")
                return None

    # ---------- per location ----------
    for i, (brand, dealer, Location, location_path) in enumerate(all_locations):
        progress_bar.progress((i + 1) / max(total_locations, 1))
        status_text.text(f"Generating reports for {Location} ({i+1}/{total_locations})...")

        # reset per-location collectors
        mrn_list, stock_list, po_list = [], [], []

        for fname in os.listdir(location_path):
            fpath = os.path.join(location_path, fname)
            if not os.path.isfile(fpath):
                continue

            fl = fname.lower().strip()
            if fl.startswith('mrn'):
                df = read_file(fpath)
                if df is not None:
                    df['Brand'] = brand; df['Dealer'] = dealer; df['Location'] = Location; df['_Sourcefile_'] = fname
                    mrn_list.append(df)

            elif fl.startswith('stock'):
                try:
                    sdf = pd.read_table(fpath, encoding='utf-16')
                    df = pd.concat([sdf], ignore_index=True)
                    df = df[(df['Part Number'].notna()) & (df['Part Number'] != '')]
                    df['Brand'] = brand; df['Dealer'] = dealer; df['Location'] = Location; df['_Sourcefile_'] = fname
                    stock_list.append(df)
                except Exception as e:
                    st.warning(f"Stock read failed for {fname}: {e}")

            elif fl.startswith('po'):
                df = read_file(fpath)
                if df is not None:
                    df['Brand'] = brand; df['Dealer'] = dealer; df['Location'] = Location; df['_Sourcefile_'] = fname
                    po_list.append(df)

        # stk
        if stock_list:
            Stock_df = pd.concat(stock_list, ignore_index=True)
            if 'Inventory Location Name' in Stock_df.columns:
               #  Stock_df = Stock_df['Inventory Location Name'].astype(str)
               #  pat = r'(?<=-)(?:[A-Z]{2}\d{6}|[A-Z]{2}\d{2}[A-Z]{2}\d{2})(?=-)
               # # Stock_df['Location_code'] = s.str.upper().str.extract(pat, expand=False)
               #  rx = re.compile(pat)
               #  Stock_df['Location_code'] = s.apply(lambda x: (m.group(0) if (m := rx.search(x)) else None) if isinstance(x, str) else None)

              
                pattern = r'-([A-Z]{2}\d{6}|[A-Z]{2}\d{2}[A-Z]{2}\d{2})'
                Stock_df['Location_code'] = Stock_df['Inventory Location Name'].apply(lambda x: re.search(pattern, x).group(1) if re.search(pattern, x) else None)


          
                #Stock_df['Inventory Location Name'].astype(str).str.split('-').str[4].fillna('')
                
            else:
                Stock_df['Location_code'] = ''

            if 'Quantity' in Stock_df.columns:
                Stock_df['Quantity'] = pd.to_numeric(
                    Stock_df['Quantity'].astype(str).str.replace(',', '', regex=False), errors='coerce'
                ).fillna(0)
            else:
                Stock_df['Quantity'] = 0

            merged = L_master.merge(Stock_df, left_on='Code', right_on='Location_code', how='inner') if not L_master.empty else Stock_df

            mask = (merged.get('Location_code').notnull()) & \
                   (merged.get('Availability', '').eq('On Hand')) & \
                   (merged.get('Quantity', 0) > 0)

            stk = merged.loc[mask, ['Brand','Dealer','Location_x','Location_code','Code','Part Number','Quantity']].copy()
            stk.rename(columns={'Location_x':'Location', 'Quantity':'Qty', 'Part Number':'PartNumber'}, inplace=True)

            # **Generate report per dealer**
            stk_filename = f"stock_{brand}_{dealer}_{Location}.xlsx"
            _store_xlsx(stk_filename, stk)
        # if stock_list:
        #     Stock_df = pd.concat(stock_list, ignore_index=True)
        
        #     # --- Extract Location_code safely from "Inventory Location Name" ---
        #     if 'Inventory Location Name' in Stock_df.columns:
        #         s = Stock_df['Inventory Location Name'].astype('string').str.upper()
        #         # code between dashes: BR230001  or  BR23AH01
        #         pat = r'(?<=-)(?:[A-Z]{2}\d{6}|[A-Z]{2}\d{2}[A-Z]{2}\d{2})(?=-)'
        #         Stock_df['Location_code'] = s.str.extract(pat, expand=False)
        #     else:
        #         Stock_df['Location_code'] = pd.Series(pd.NA, index=Stock_df.index)
        
        #     # --- Quantity to numeric (handles commas & blanks) ---
        #     if 'Quantity' in Stock_df.columns:
        #         Stock_df['Quantity'] = (
        #             pd.to_numeric(
        #                 Stock_df['Quantity'].astype(str).str.replace(',', '', regex=False),
        #                 errors='coerce'
        #             ).fillna(0)
        #         )
        #     else:
        #         Stock_df['Quantity'] = 0
        
        #     # --- Merge ---
        #     merged = (
        #         L_master.merge(Stock_df, left_on='Code', right_on='Location_code', how='inner')
        #         if not L_master.empty else Stock_df
        #     )
        
        #     # --- Build mask robustly ---
        #     avail = merged['Availability'] if 'Availability' in merged.columns else ''
        #     qty = merged['Quantity'] if 'Quantity' in merged.columns else 0
        #     mask = merged['Location_code'].notna() & (avail == 'On Hand') & (qty > 0)
        
        #     # --- Column picking that survives merge suffixes ---
        #     loc_col = 'Location_x' if 'Location_x' in merged.columns else ('Location' if 'Location' in merged.columns else None)
        #     part_col = 'Part Number' if 'Part Number' in merged.columns else ('PartNumber' if 'PartNumber' in merged.columns else None)
        
        #     cols = ['Brand', 'Dealer', 'Location_code', 'Code', 'Quantity']
        #     if loc_col: cols.append(loc_col)
        #     if part_col: cols.append(part_col)
        #     cols = [c for c in cols if c in merged.columns]  # keep only those that exist
        
        #     stk = merged.loc[mask, cols].copy()
        #     # nice headers
        #     rename_map = {}
        #     if loc_col: rename_map[loc_col] = 'Location'
        #     if part_col: rename_map[part_col] = 'PartNumber'
        #     rename_map['Quantity'] = 'Qty'
        #     stk.rename(columns=rename_map, inplace=True)
        
        #     # --- Save ---
        #     stk_filename = f"stock_{brand}_{dealer}_{Location}.xlsx"  # assumes these vars exist
        #     _store_xlsx(stk_filename, stk)

        
        if mrn_list:
            Mrn_df = pd.concat(mrn_list, ignore_index=True)
            merged = L_master.merge(Mrn_df, left_on='Code', right_on='Network Code', how='inner') if not L_master.empty else Mrn_df

            mask = (merged.get('Code').notnull()) & \
                   (merged.get('MRNs Actual Received Qty', 0) > 0) & \
                   (merged.get('Supplier Name', '') == 'HCIL')

            mrn_D = merged.loc[mask, ['Brand','Dealer','Location_x','Part Number','Order Number','Order Date','MRNs Actual Received Qty']].copy()
            mrn_D['OEMInvoiceNo'] = ''
            mrn_D['OEMInvoiceDate'] = ''
            mrn_D['OEMInvoiceQty'] = ''
            mrn_D['MRNNumber'] = ''
            mrn_D['MRNDate'] = ''
            mrn_D.rename(columns={
                'Location_x':'Location', 'Part Number':'PartNumber',
                'Order Number':'OrderNumber', 'Order Date':'OrderDate',
                'MRNs Actual Received Qty':'ReceiptQty'
            }, inplace=True)

            # **Generate report per dealer**
            mrn_filename = f"Mrn_{brand}_{dealer}_{Location}.xlsx"
            _store_xlsx(mrn_filename, mrn_D)

        if po_list:
            po_df = pd.concat(po_list, ignore_index=True)
            Po_df = L_master.merge(po_df, left_on='Code', right_on='Network Code', how='inner') if not L_master.empty else po_df

            mask = (Po_df.get('Order Status','') == 'Sent To HCIL') & (pd.to_numeric(Po_df.get('Quantity Requested', 0), errors='coerce') > 0)
            Po_D = Po_df.loc[mask, ['Brand','Dealer','Location_x','Part Number','Order Number','Order Date','Quantity Requested']].copy()
            Po_D.rename(columns={
                'Location_x':'Location', 'Part Number':'PartNumber',
                'Order Number':'OrderNumber', 'Order Date':'OrderDate', 'Quantity Requested':'POQty'
            }, inplace=True)
            Po_D['OEMInvoiceNo'] = ''
            Po_D['OEMInvoiceDate'] = ''
            Po_D['OEMInvoiceQty'] = ''
            Po_D.drop_duplicates(inplace=True)

            # **Generate report per dealer**
            po_filename = f"Po_{brand}_{dealer}_{Location}.xlsx"
            _store_xlsx(po_filename, Po_D)

    if validation_errors:
        st.warning("⚠ Validation issues found:")
        for error in validation_errors:
            st.write(f"- {error}")

    st.success("🎉 Reports generated successfully!")
    st.subheader("📥 Download Reports")

    report_types = {
        'OEM':   [k for k in file_bytes.keys() if k.startswith('OEM_')],
        'Stock': [k for k in file_bytes.keys() if k.lower().startswith('stock_')],
        'Mrn':   [k for k in file_bytes.keys() if k.startswith('Mrn_')],
        'PO':    [k for k in file_bytes.keys() if k.startswith('Po_')],
    }

    # show previews + individual downloads
    for rtype, name_list in report_types.items():
        if name_list:
            with st.expander(f"📂 {rtype} Reports ({len(name_list)})", expanded=False):
                for fname in name_list:
                    df = previews.get(fname)
                    if df is not None and not df.empty:
                        st.markdown(f"### 📄 {fname}")
                        st.dataframe(df.head(5))

                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False, sheet_name='Sheet1')
                        st.download_button(
                            label="⬇ Download Excel",
                            data=excel_buffer.getvalue(),
                            file_name=fname,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_{fname}"
                        )
                    else:
                        st.warning(f"⚠ No data for {fname}")

    # ---------- Create ZIP for all reports ----------
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add each file to the ZIP
        for file_name, file_data in file_bytes.items():
            zipf.writestr(file_name, file_data)

    # ---------- UI: Download ZIP ----------
    st.download_button(
        label="📦 Download Combined Dealer Reports ZIP",
        data=zip_buffer.getvalue(),
        file_name="Combined_Dealerwise_Reports.zip",
        mime="application/zip"
    )

#    st.success("🎉 Reports generated successfully!")








