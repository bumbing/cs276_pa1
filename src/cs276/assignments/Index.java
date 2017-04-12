package cs276.assignments;

import cs276.util.Pair;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict 
		= new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict
		= new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict
		= new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue
		= new LinkedList<File>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	
	/* 
	 * Write a posting list to the given file 
	 * You should record the file position of this posting list
	 * so that you can read it back during retrieval
	 * 
	 * */
	private static void writePosting(FileChannel fc, PostingList posting)
            throws Throwable {
        postingDict.put(posting.getTermId(), new Pair<Long, Integer>(fc
                .position(), (posting.getList()).size()));
        index.writePosting(fc, posting);
	}

	public static void main(String[] args) throws Throwable {
		/* Parse command line */
		if (args.length != 3) {
			System.err
					.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
			return;
		}

		/* Get index */
		String className = "cs276.assignments." + args[0] + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		String root = args[1];
		File rootdir = new File(root);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + root);
			return;
		}

		/* Get output directory */
		String output = args[2];
		File outdir = new File(output);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + output);
			return;
		}

		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return;
			}
		}

		/* A filter to get rid of all files starting with .*/
		FileFilter filter = new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				String name = pathname.getName();
				return !name.startsWith(".");
			}
		};

		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles(filter);

		/* For each block */
		for (File block : dirlist) {
			File blockFile = new File(output, block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(root, block.getName());
			File[] filelist = blockDir.listFiles(filter);

			Map<Integer, ArrayList<Integer>> termInCurrentBlock = new HashMap<Integer, ArrayList<Integer>>();
			
			/* For each file */
			for (File file : filelist) {
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
				docDict.put(fileName, docIdCounter++);
				
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						int termID = termDict.getOrDefault(token, wordIdCounter);
					    if(!termDict.containsKey(token)) {
					        termDict.put(token, wordIdCounter++);
                        }
                        if(!termInCurrentBlock.containsKey(termID)) {
					        termInCurrentBlock.put(termID, new ArrayList<Integer>());
                            termInCurrentBlock.get(termID).add(docIdCounter);
                        }
					}
				}
				reader.close();
			}

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return;
			}
			
			RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
			FileChannel fileChannel = bfc.getChannel();

            for(Map.Entry<Integer, ArrayList<Integer>> list : termInCurrentBlock.entrySet()) {
                PostingList postingList = new PostingList(list.getKey(), list.getValue());
                try {
                    index.writePosting(fileChannel, postingList);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
            fileChannel.close();
			
			bfc.close();
		}

		/* Required: output total number of files. */
		System.out.println(totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1)
				break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();
			
			File combfile = new File(output, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");

			FileChannel f1 = bf1.getChannel();
            FileChannel f2 = bf2.getChannel();
            PostingList pl1 = index.readPosting(f1);
            PostingList pl2 = index.readPosting(f2);
            FileChannel outputStream = mf.getChannel();

            while(pl1 != null || pl2 != null) {
                if(pl1 == null) {
                    //read single posting list.
                    writePosting(outputStream, pl2);
                    pl2 = index.readPosting(f2);
                } else if (pl2 == null) {
                    writePosting(outputStream, pl1);
                    pl2 = index.readPosting(f1);
                } else if (pl1.getTermId() == pl2.getTermId()) {
                    //merge lists.
                    ArrayList<Integer> mergerdList = Merge_Posting_list(pl1.getList(), pl2.getList());
                    writePosting(outputStream, new PostingList(pl1.getTermId(), mergerdList));
                    pl1 = index.readPosting(f1);
                    pl2 = index.readPosting(f2);
                } else if (pl1.getTermId() > pl2.getTermId()) {
                    writePosting(outputStream, pl2);
                    pl2 = index.readPosting(f2);
                } else {
                    writePosting(outputStream, pl1);
                    pl1 = index.readPosting(f1);
                }
            }

			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(output, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
				output, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
				output, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
				output, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
					+ "\t" + postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();
	}

    private static ArrayList<Integer> Merge_Posting_list(List<Integer> list,
                                                        List<Integer> list2) {

        int line1_index = 0;
        int line2_index = 0;
        ArrayList<Integer> merged_list = new ArrayList<Integer>();
        while (true) {
            if (line1_index >= list.size()) {
                for (int i = line2_index; i < list2.size(); i++) {
                    merged_list.add(list2.get(i));
                }
                break;
            }

            if (line2_index >= list2.size()) {
                for (int i = line1_index; i < list.size(); i++) {
                    merged_list.add(list.get(i));
                }
                break;
            }

            int current_line1 = list.get(line1_index);
            int current_line2 = list2.get(line2_index);

            if (current_line1 == current_line2) {
                merged_list.add(current_line1);
                current_line1++;
                current_line2++;
            }

            else if (current_line1 < current_line2) {
                merged_list.add(current_line1);
                line1_index++;
            } else {
                merged_list.add(current_line2);
                line2_index++;
            }
        }
        return merged_list;
    }
}
