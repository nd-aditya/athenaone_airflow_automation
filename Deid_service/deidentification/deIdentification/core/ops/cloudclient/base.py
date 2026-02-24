import abc


class CloudClient(abc.ABC):
    @abc.abstractmethod
    def download_file(self, source, filename):
        """ Returns file data of object_key from bucket_name """

    @abc.abstractmethod
    def upload_file(self, source, filename, data, content_type):
        """ Uploads file as object_key in bucket_name """

    @abc.abstractmethod
    def upload_file_from_fs(self, source, filename, full_path_to_file, content_type):
        """ Uploads local file with file_path as object_key in bucket_name """

    @abc.abstractmethod
    def exists(self, source, filename):
        """ Return true if file exists """

    @abc.abstractmethod
    def delete_file(self, source, filename):
        """ Deletes the specified file """

    @abc.abstractmethod
    def delete_folder(self, source, folder):
        """ Deletes the specified folder """

