from zipfile import ZipFile
import h5py
import numpy as np


def write_hdf5(path_zip: str, path_hdf: str, name_hdf: str, data_type: str, title_size: int, data_size: int):
    with ZipFile(path_zip, 'r') as zip_obj:
        n_columns = len(zip_obj.namelist())
        data_array_size = data_size * np.dtype(data_type).itemsize
        n_rows = zip_obj.filelist[0].file_size // (
            data_array_size + title_size)

        with h5py.File(path_hdf, "w") as hdf_obj:
            dset = hdf_obj.create_dataset(
                name_hdf, (n_rows, n_columns, data_size), data_type)
            for id_col, nm_file in enumerate(zip_obj.namelist()):
                with zip_obj.open(nm_file) as file:
                    for id_column in range(n_rows):
                        file.seek(title_size, 1)
                        dset[id_column, id_col, :] = np.frombuffer(
                            file.read(data_array_size), data_type)
