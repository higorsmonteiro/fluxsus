import os
import struct
import codecs

from simpledbf import Dbf5
from simpledbf.simpledbf import DbfBase


# -- copy from simpledbf library excluded assertion error --
class DBFIX(DbfBase):
    '''
    DBF version 5 file processing object.

    This class defines the methods necessary for reading the header and
    records from a version 5 DBF file.  Much of this code is based on an
    `ActiveState DBF example`_, which only worked for Python2.

    .. ActiveState DBF example: http://code.activestate.com/recipes/
            362715-dbf-reader-and-writer/

    Parameters
    ----------

    dbf : string
        The name (with optional path) of the DBF file.

    codec : string, optional
        The codec to use when decoding text-based records. The default is
        'utf-8'. See Python's `codec` standard lib module for other options.

    Attributes
    ----------

    dbf : string
        The input file name.

    f : file object
        The opened DBF file object

    numrec : int
        The number of records contained in this file.
    
    lenheader : int
        The length of the file header in bytes.

    numfields : int
        The number of data columns.

    fields : list of tuples
        Column descriptions as a tuple: (Name, Type, # of bytes).

    columns : list
        The names of the data columns.

    fmt : string
        The format string that is used to unpack each record from the file.

    fmtsiz : int
        The size of each record in bytes.
    '''
    def __init__(self, dbf, codec='utf-8'):
        self._enc = codec
        path, name = os.path.split(dbf)
        self.dbf = name
        # Escape quotes, set by indiviual runners
        self._esc = None
        # Reading as binary so bytes will always be returned
        self.f = open(dbf, 'rb')

        self.numrec, self.lenheader = struct.unpack('<xxxxLH22x', 
                self.f.read(32))    
        self.numfields = (self.lenheader - 33) // 32

        # The first field is always a one byte deletion flag
        fields = [('DeletionFlag', 'C', 1),]
        for fieldno in range(self.numfields):
            name, typ, size = struct.unpack('<11sc4xB15x', self.f.read(32))
            # eliminate NUL bytes from name string  
            name = name.strip(b'\x00')        
            fields.append((name.decode(self._enc), typ.decode(self._enc), size))
        self.fields = fields
        # Get the names only for DataFrame generation, skip delete flag
        self.columns = [f[0] for f in self.fields[1:]]
        
        terminator = self.f.read(1)
        #assert terminator == b'\r'
     
        # Make a format string for extracting the data. In version 5 DBF, all
        # fields are some sort of structured string
        self.fmt = ''.join(['{:d}s'.format(fieldinfo[2]) for 
                            fieldinfo in self.fields])
        self.fmtsiz = struct.calcsize(self.fmt)

    def _get_recs(self, chunk=None):
        '''Generator that returns individual records.

        Parameters
        ----------
        chunk : int, optional
            Number of records to return as a single chunk. Default 'None',
            which uses all records.
        '''
        if chunk == None:
            chunk = self.numrec

        for i in range(chunk):
            # Extract a single record
            record = struct.unpack(self.fmt, self.f.read(self.fmtsiz))
            # If delete byte is not a space, record was deleted so skip
            if record[0] != b' ': 
                continue  
            
            # Save the column types for later
            self._dtypes = {}
            result = []
            for idx, value in enumerate(record):
                name, typ, size = self.fields[idx]
                if name == 'DeletionFlag':
                    continue

                # String (character) types, remove excess white space
                if typ == "C":
                    if name not in self._dtypes:
                        self._dtypes[name] = "str"
                    value = value.strip()
                    # Convert empty strings to NaN
                    if value == b'':
                        value = self._na
                    else:
                        value = value.decode(self._enc)
                        # Escape quoted characters
                        if self._esc:
                            value = value.replace('"', self._esc + '"')

                # Numeric type. Stored as string
                elif typ == "N":
                    # A decimal should indicate a float
                    if b'.' in value:
                        if name not in self._dtypes:
                            self._dtypes[name] = "float"
                        value = float(value)
                    # No decimal, probably an integer, but if that fails,
                    # probably NaN
                    else:
                        try:
                            value = int(value)
                            if name not in self._dtypes:
                                self._dtypes[name] = "int"
                        except:
                            # I changed this for SQL->Pandas conversion
                            # Otherwise floats were not showing up correctly
                            value = float('nan')

                # Date stores as string "YYYYMMDD", convert to datetime
                elif typ == 'D':
                    try:
                        y, m, d = int(value[:4]), int(value[4:6]), \
                                  int(value[6:8])
                        if name not in self._dtypes:
                            self._dtypes[name] = "date"
                    except:
                        value = self._na
                    else:
                        value = datetime.date(y, m, d)

                # Booleans can have multiple entry values
                elif typ == 'L':
                    if name not in self._dtypes:
                        self._dtypes[name] = "bool"
                    if value in b'TyTt':
                        value = True
                    elif value in b'NnFf':
                        value = False
                    # '?' indicates an empty value, convert this to NaN
                    else:
                        value = self._na

                # Floating points are also stored as strings.
                elif typ == 'F':
                    if name not in self._dtypes:
                        self._dtypes[name] = "float"
                    try:
                        value = float(value)
                    except:
                        value = float('nan')

                else:
                    err = 'Column type "{}" not yet supported.'
                    raise ValueError(err.format(value))

                result.append(value)
            yield result