use crate::arrow2::error::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::write::{FileWriter, RowGroupIterator};

/// Internal function to write a buffer of data in Arrow IPC File format to a Parquet file using
/// the arrow2 and parquet2 crates
pub fn write_parquet(
    batches: impl Iterator<Item = Chunk<Box<dyn Array>>>,
    schema: Schema,
    writer_properties: crate::arrow2::writer_properties::WriterProperties,
) -> Result<Vec<u8>> {
    // Create Parquet writer
    let mut output_file: Vec<u8> = vec![];
    let options = writer_properties.get_write_options();
    let encoding = writer_properties.get_encoding();

    let mut parquet_writer = FileWriter::try_new(&mut output_file, schema.clone(), options)?;

    for chunk in batches {
        let iter = vec![Ok(chunk)];

        // Need to create an encoding for each column
        let mut encodings = vec![];
        for _ in &schema.fields {
            // Note, the nested encoding is for nested Parquet columns
            // Here we assume columns are not nested
            encodings.push(vec![encoding]);
        }

        let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings);

        if let Ok(row_group_iterator) = row_groups {
            for maybe_group in row_group_iterator {
                let group = maybe_group?;
                parquet_writer.write(group)?;
            }
        }
    }
    let _size = parquet_writer.end(None)?;
    Ok(output_file)
}
