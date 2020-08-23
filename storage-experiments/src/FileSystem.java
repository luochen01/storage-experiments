import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

interface IFile {
    String name();

    boolean isDirectory();

    int getType();

    long getSize();

    List<IFile> getSubFiles();

}

abstract class AbstractFile implements IFile {
    protected String name;

    public AbstractFile(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }
}

class File extends AbstractFile {
    private final int type;
    private final long size;

    public File(String name, int type, long size) {
        super(name);
        this.type = type;
        this.size = size;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public List<IFile> getSubFiles() {
        return Collections.emptyList();
    }
}

class Directory extends AbstractFile {

    private final List<IFile> subFiles = new ArrayList<>();

    public Directory(String name) {
        super(name);
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public int getType() {
        return -1;
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public List<IFile> getSubFiles() {
        return subFiles;
    }
}

@FunctionalInterface
interface IFilter {
    boolean isValid(IFile file);
}

class SizeFilter implements IFilter {
    public enum Condition {
        EQ,
        LE,
        GE
    }

    private final Condition cond;
    private final int size;

    public SizeFilter(Condition cond, int size) {
        this.cond = cond;
        this.size = size;
    }

    @Override
    public boolean isValid(IFile file) {
        if (file.isDirectory()) {
            return false;
        }
        switch (cond) {
            case EQ:
                return file.getSize() == size;
            case GE:
                return file.getSize() >= size;
            case LE:
                return file.getSize() <= size;
        }
        return false;
    }
}

class TypeFilter implements IFilter {
    private final int type;

    public TypeFilter(int type) {
        this.type = type;
    }

    @Override
    public boolean isValid(IFile file) {
        return !file.isDirectory() && file.getType() == type;
    }
}

class AndFilter implements IFilter {
    private final IFilter[] filters;

    public AndFilter(IFilter[] filters) {
        this.filters = filters;
    }

    @Override
    public boolean isValid(IFile file) {
        for (IFilter filter : filters) {
            if (!filter.isValid(file)) {
                return false;
            }
        }
        return true;
    }

}

class FileSystem {
    final IFile root = new Directory("/");

    public List<IFile> find(IFilter filter) {
        List<IFile> result = new ArrayList<>();

        collect(result, root, filter);
        return result;
    }

    private void collect(List<IFile> result, IFile file, IFilter filter) {
        if (filter.isValid(file)) {
            result.add(file);
        }
        for (IFile child : file.getSubFiles()) {
            collect(result, child, filter);
        }

    }

}
