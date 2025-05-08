from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import laspy
import lazrs
import numpy as np
import io
import tempfile
import os


class LazToCsvProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.6'
        description = 'Reads a .laz file from FlowFile content and converts it to CSV.'
        dependencies = ['laspy', 'lazrs', 'numpy']

    MAX_RECORDS = PropertyDescriptor(
        name="Max Records",
        description="Maximum number of LAS points to read from the LAZ file",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="100000",
        required=False
    )

    CHUNK_RECORDS = PropertyDescriptor(
        name="Chunk Records",
        description="Rows processed per sub-chunk to avoid high memory usage",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="10000",
        required=False
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.MAX_RECORDS, self.CHUNK_RECORDS]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        try:
            # Read binary content from FlowFile
            laz_data = flowfile.getContentsAsBytes()

            if not laz_data:
                raise Exception("FlowFile content is empty.")

            # Retrieve property values
            max_records_val = context.getProperty("Max Records").getValue()
            chunk_records_val = context.getProperty("Chunk Records").getValue()
            max_records = int(max_records_val) if max_records_val else 100000
            chunk_records = int(chunk_records_val) if chunk_records_val else 10000

            # Write FlowFile content to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".laz") as tmp_file:
                tmp_file.write(laz_data)
                tmp_file_path = tmp_file.name

            total_points = 0
            output = io.StringIO()
            output.write("x,y,z,intensity,return_num,classification\n")

            # Read the .laz file in chunks
            with laspy.open(tmp_file_path, laz_backend=laspy.LazBackend.Lazrs) as laz_reader:
                for las in laz_reader.chunk_iterator(max_records):
                    if las is None or len(las) == 0:
                        continue

                    total_points += len(las)

                    # Process sub-chunks of the data
                    for i in range(0, len(las), chunk_records):
                        end = i + chunk_records

                        # Create a structured NumPy array
                        structured_array = np.array(
                            list(zip(
                                las.x[i:end], las.y[i:end], las.z[i:end],
                                las.intensity[i:end],
                                las.return_number[i:end],
                                las.classification[i:end]
                            )),
                            dtype=[
                                ('x', 'f8'), ('y', 'f8'), ('z', 'f8'),
                                ('intensity', 'i4'), ('return_num', 'i4'),
                                ('classification', 'i4')
                            ]
                        )

                        # Write comma-separated lines
                        for row in structured_array:
                            output.write(f"{row['x']},{row['y']},{row['z']},{row['intensity']},{row['return_num']},{row['classification']}\n")

            os.remove(tmp_file_path)

            if total_points == 0:
                raise Exception("No points found in LAZ file.")

            output_str = output.getvalue()
            output.close()

            return FlowFileTransformResult(
                contents=output_str.encode("utf-8"),
                attributes={
                    "laz.converted": "true",
                    "point.count": str(total_points)
                },
                relationship="success"
            )

        except Exception as e:
            return FlowFileTransformResult(
                contents=str(e).encode("utf-8"),
                attributes={"error": str(e)},
                relationship="failure"
            )

