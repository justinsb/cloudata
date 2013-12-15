package com.cloudata.git.jgit;

import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;

public class CloudDfsPackDescription extends DfsPackDescription {

	CloudDfsPackDescription(String name, DfsRepositoryDescription repoDesc) {
		super(repoDesc, name);
	}

}
