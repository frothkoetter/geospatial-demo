from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import laspy
import lazrs
import numpy as np
import io
import tempfile
import os

class laz2csvprocessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1'
        description = 'Reads a .laz file from FlowFile content and converts it to CSV.'
        dependencies = ['laspy','lazrs', 'numpy'] 

    # Property must be declared at the class level for NiFi to parse it
    MAX_RECORDS = PropertyDescriptor(
        name="Max Records",
        description="Maximum number of LAS points to read from the LAZ file.",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="1500000",
        required=False
    )

    def __init__(self, **kwargs):
        # Remove unexpected keys
        kwargs.pop("jvm", None)

        super().__init__(**kwargs)

        self.descriptors = [self.MAX_RECORDS]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        try:
            # Read the binary content of the FlowFile
            laz_data = flowfile.getContentsAsBytes()

            if not laz_data:
                raise Exception("FlowFile content is empty.")

            # Get parameter from NiFi processor properties 
            max_records_val = context.getProperty("Max Records").getValue()
            max_records = int(max_records_val) if max_records_val is not None else 10000

            # Write content to a temporary .laz file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".laz") as tmp_file:
                tmp_file.write(laz_data)
                tmp_file_path = tmp_file.name

            # Process with laspy
            with laspy.open(tmp_file_path,laz_backend=laspy.LazBackend.Lazrs) as laz_reader:
                # Use chunk iterator to read max points
                chunk_iter = laz_reader.chunk_iterator(max_records)
                las = next(chunk_iter)
        
                if las is None or len(las) == 0:
                    raise Exception("No points found in LAZ file.")

                # Convert to structured numpy array
                # Extract fields
                points = np.array([
                    (x, y, z, intensity, return_num, classification)
                    for x, y, z, intensity, return_num, classification in zip(
                        las.x, las.y, las.z,
                        las.intensity,
                        las.return_number,
                        las.classification
                    )
                ], dtype=[
                    ('x', 'f8'), ('y', 'f8'), ('z', 'f8'),
                    ('intensity', 'i4'), ('return_num', 'i4'), 
                    ('classification', 'i4')
                ])

            os.remove(tmp_file_path)  # Clean up temp file

            # Convert to CSV format
            output = io.StringIO()
            output.write("x,y,z,intensity,return_num,classification\n")
            for pt in points:
               output.write("{},{},{},{},{},{}\n".format(*pt))

            output_str = output.getvalue()
            output.close()

            return FlowFileTransformResult(
                contents=output_str.encode("utf-8"),
                attributes={
                    "laz.converted": "true",
                    "point.count": str(len(points))
                },
                relationship="success"
            )

        except Exception as e:
            return FlowFileTransformResult(
                contents=str(e).encode("utf-8"),
                attributes={"error": str(e)},
                relationship="failure"
            )

