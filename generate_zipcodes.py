import zipcodes

def generate_zip_codes():
    # Get all ZIP codes from the database
    all_zip_data = zipcodes.list_all()
    
    # Extract just the ZIP codes and ensure they are 5 digits
    zip_codes = [str(z['zip_code']).zfill(5) for z in all_zip_data]
    
    # Sort and remove duplicates
    zip_codes = sorted(list(set(zip_codes)))
    
    return zip_codes

# Generate and format the zip codes
zip_codes = generate_zip_codes()
print(f"Total zip codes: {len(zip_codes)}")
print(f"First 10 zip codes: {zip_codes[:10]}")
print(f"Last 10 zip codes: {zip_codes[-10:]}")

# Format in the requested style
formatted_output = "ZIP_CODES = [" + ", ".join([f'"{zip_code}"' for zip_code in zip_codes]) + "]"

# Write to a file
with open("contiguous_usa_zip_codes.py", "w") as f:
    f.write(formatted_output)

print(f"Complete list of {len(zip_codes)} zip codes written to contiguous_usa_zip_codes.py")