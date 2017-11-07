package loadjson.utils;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseOptions;
import com.couchbase.lite.Document;
import com.couchbase.lite.JavaContext;
import com.couchbase.lite.Manager;
import com.couchbase.lite.SavedRevision;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class JsonLoad {

	private static final String projectFolder = "/Users/junjie.gan/Desktop/testJson/";
	ScriptEngineManager mgr = new ScriptEngineManager();
	ScriptEngine engine = mgr.getEngineByName("JavaScript");
	ObjectMapper jacksonMapper = new ObjectMapper();

	public static void main(String args[]) throws Exception {

		JsonLoad jsonLoad = new JsonLoad();

		JsonNode documents = jsonLoad.getDocuments();
		boolean result = jsonLoad.storeDocumentsToDB(documents);

		System.out.println("DB Generate result: " + (result ? "Success" : "Failed"));
	}

	private JsonNode getDocuments() throws Exception {

		File folder = new File(projectFolder);
		if (!folder.isDirectory()) {
			throw new Exception("The project folder : " + projectFolder + " is not valid.");
		}
		Collection<File> allfiles = Lists
				.newArrayList(FileUtils.listFiles(folder, new String[] { "js", "js.txt" }, true));
		for (File file : allfiles) {
			String content = FileUtils.readFileToString(file);
			engine.eval(content);
		}
		engine.eval("createJsonFileFromDocuments");

		JsonNode documents = jacksonMapper.valueToTree(engine.get("documents"));

		return documents;
	}

	private boolean storeDocumentsToDB(JsonNode documents) throws Exception {

		String dbname = "questionnaire";
		DatabaseOptions options = new DatabaseOptions();
		options.setCreate(true);
		Manager manager = new Manager(new CustomJavaContext(), Manager.DEFAULT_OPTIONS);

		manager.openDatabase(dbname, options).delete();

		Database database = manager.openDatabase(dbname, options);

		List<String> allKeys = Lists.newArrayList(documents.fieldNames());
		ArrayNode arrayNode = ((ObjectNode) documents).putArray("allKeys");
		allKeys.forEach(e -> arrayNode.add(e));

		Lists.newArrayList(documents.fields()).forEach(each -> {
			JsonNode value = each.getValue();

			// retrieve existing doc from db, if not exist, a new doc return
			Document document = database.getDocument(each.getKey());

			// if doc is old, copy the revision id to the json new copy
			if (StringUtils.isNotBlank(document.getCurrentRevisionId())) {
				((ObjectNode) value).put("_rev", document.getCurrentRevisionId());
			}

			// write the document to the database
			try {
				@SuppressWarnings("unchecked")
				SavedRevision newRevision = document.putProperties(jacksonMapper.convertValue(each, Map.class));
				if (newRevision == null || newRevision.isGone()) {
					throw new Exception("Failed to save document : " + each.getKey());
				}
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
		});
		
		return true;
	}

}

class CustomJavaContext extends JavaContext {
	@Override
	public File getRootDirectory() {
		String rootDirectoryPath = System.getProperty("user.dir");
		return new File(rootDirectoryPath, "target");
	}
}
