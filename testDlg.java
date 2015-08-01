import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.net.URI;

/**
 * 通过GUI实现了文件的上传、查看和删除功能
 */
public class testDlg extends JDialog {
    ImageIcon imageIcon = new ImageIcon("panda.ico");
    Configuration conf;
    FileSystem hdfs;
    FileSystem local;
    String HOSTNAME = "192.168.59.128";
    private JPanel contentPane;
    private JButton buttonOK;
    private JButton buttonCancel;
    private JTextField textField1;
    private JTextField textField2;
    private JTree tree1;
    private JButton updateButton;
    private JCheckBox checkBox1;

    public testDlg() throws IOException {
        setContentPane(contentPane);
        setModal(true);
        getRootPane().setDefaultButton(buttonOK);
        this.setIconImage(imageIcon.getImage());
        this.setAlwaysOnTop(true);
        this.setName("HDFS Manager");

        buttonOK.addActionListener(e -> onOK());
        buttonCancel.addActionListener(e -> onCancel());
// call onCancel() when cross is clicked
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                onCancel();
            }
        });

// call onCancel() on ESCAPE
        contentPane.registerKeyboardAction(e -> onCancel(), KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
        updateButton.addActionListener(e -> updateTree());

        conf = new Configuration();
//        conf.set("fs.default.name", "hdfs://192.168.59.128:9000");
//        conf.set("mapreduce.jobtracker.address", "192.168.59.128:9001");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "192.168.59.139");

        hdfs = FileSystem.get(URI.create(HOSTNAME), conf);
        local = FileSystem.getLocal(conf);

        updateTree();
        tree1.addTreeSelectionListener(e -> {

            TreePath path = e.getPath();    //返回的是一个TreeNode型数组
            Object[] path1 = path.getPath();
            String filePath = "";
            for (int i = 0; i < path1.length; i++) {
                if (i == 0) {
                    filePath += "/";
                }
                else{
                    filePath += "/" + path1[i];
                }
            }
            filePath = HOSTNAME + filePath;

            if (checkBox1.isSelected()) {
                //      Configuration conf = new Configuration();
                try {
                    //         FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"), conf);
                    Path delef = new Path(filePath);
                    boolean isDeleted = hdfs.delete(delef, true);
                    System.out.println("Delete?\n" + isDeleted);
                } catch (IOException ee) {
                    ee.printStackTrace();
                }
            } else {
                System.out.println(filePath);
            }

        });
    }

    public static void main(String[] args) throws IOException {
        testDlg dialog = new testDlg();
        dialog.pack();
        dialog.setVisible(true);
        System.exit(0);
    }

    private void onOK() {
        String localFile = textField1.getText();
        String HDFSFile = textField2.getText();
        uploadFile(localFile, HDFSFile);
  //      dispose();
    }

    private void onCancel() {
// add your code here if necessary
        dispose();
    }

    private void uploadFile(String localFile,String HDFS_File){
        //   Configuration conf;
        try {
            //        conf = new Configuration();
            //    FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"), conf);

            Path inputDir = new Path(localFile);
            Path hdfsFile = new Path(HDFS_File);

            FSDataOutputStream out;
            FSDataInputStream in = local.open(inputDir);
            out = hdfs.create(hdfsFile);

            byte buffer[] = new byte[256];
            int bytesRead;
            while (0 < (bytesRead = in.read(buffer))) {
                out.write(buffer, 0, bytesRead);
            }
            out.close();
            in.close();
            System.out.println("end");

        }catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    private void updateTree() {

//        Configuration conf;
        try {
//            conf = new Configuration();
//            FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"), conf);
            FileStatus[] status = hdfs.listStatus(new Path("/"));
            DefaultMutableTreeNode top = getFile2node(hdfs, status, "/");
            DefaultTreeModel treeModel = new DefaultTreeModel(top);
            tree1.setModel(treeModel);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private DefaultMutableTreeNode getFile2node(FileSystem hdfs,FileStatus[] status,String name) throws IOException {
        DefaultMutableTreeNode top = new DefaultMutableTreeNode(name);
        for(FileStatus status1 : status){
            DefaultMutableTreeNode node;
            if(status1.isDirectory()){
                node = getFile2node(hdfs,hdfs.listStatus(status1.getPath()),status1.getPath().getName());
            }
            else {
                node = new DefaultMutableTreeNode(status1.getPath().getName());
            }
            top.add(node);
        }
        return top;
    }
}
