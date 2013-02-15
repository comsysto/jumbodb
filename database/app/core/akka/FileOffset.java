package core.akka;

public class FileOffset {
        int fileNameHash;
        long offset;

        public FileOffset(int fileNameHash, long offset) {
            this.fileNameHash = fileNameHash;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FileOffset that = (FileOffset) o;

            if (fileNameHash != that.fileNameHash) return false;
            if (offset != that.offset) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = fileNameHash;
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }
    }