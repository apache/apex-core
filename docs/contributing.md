# Contributing Guidelines

This project welcomes new contributors and invites everyone to participate. Our aim is to build an open community. There are many different ways to get involved:

* Adding new features, enhancements, tests or fixing bugs
* Pull request reviews
* Release management and verification
* Participation on email list
* Web site improvements
* Documentation
* Organize meetups and other events
* Publishing papers and blogs
* Present at conferences or spread the word in other ways
 
People that help with the project in any of the above categories or other ways are contributors. See the [roles](http://www.apache.org/foundation/how-it-works.html#roles) as defined by the ASF.

## Becoming a committer

Community members that make sustained, welcome contributions to the project may be invited to become a committer. Committers are voted in by the PPMC. A committer has a signed Contributor License Agreement ([CLA](http://www.apache.org/licenses/icla.txt)) on file and an apache.org address.

We expect committers to subscribe to the [project mailing lists](community.html#mailing-lists).  

A committer will be considered “emeritus/inactive” by not contributing in any form to the project for over 1 year. An emeritus committer may request reinstatement of commit access from the PPMC. Such reinstatement is subject to lazy consensus of active PPMC members.

The Podling Project Management Committee ([PPMC](http://incubator.apache.org/guides/ppmc.html)) is responsible for the oversight of the project and it also decides who to add as a PPMC member. Existing committers may be invited to become a PPMC member after consistent contribution and activity over a period of time and participation in directional and community building discussions.

## Opening Pull Requests (contributors)

The apex-core and apex-malhar repositories both have mirror repositories on github which are used to review pull requests and provide a second remote endpoint for the codebase.

1. Create a JIRA ([-core](https://issues.apache.org/jira/browse/APEXCORE/),[-malhar](https://malhar.atlassian.net/projects/MLHR/issues/)) for the work you plan to do (or assign yourself to an existing JIRA ticket)
1. Fork the ASF github mirror (one time step):
   https://github.com/apache/incubator-apex-core/  
1. Clone the **fork** on your local workspace (one time step):  
  `git clone https://github.com/{github_username}/incubator-apex-core.git`
1. Add [incubator apex core](https://github.com/apache/incubator-apex-core) as a remote repository (one time step):  
`git remote add upstream https://github.com/apache/incubator-apex-core`
1. Create a new branch from the [devel-3](https://github.com/apache/incubator-apex-core/tree/devel-3) branch. **Name your branch with the JIRA number in it, e.g. `APEXCORE-123.my-feature`.**  
`git checkout -b APEX-123.my-feature -t upstream/devel-3`  
Creating a local branch that tracks a remote makes pull easier (no need to specify the remote branch while pulling). A branch can be made to track a remote branch anytime, not necessarily at its creation by:  
`git branch -u upstream/devel-3`
1. When adding new files, please include the Apache v2.0 license header.
  - From the top level directory, run `mvn license:check -Dlicense.skip=false` to check correct header formatting.
  - Run `mvn license:format -Dlicense.skip=false` to automatically add the header when missing.
1. Once your feature is complete, submit the pull request on github against `devel-3`.
1. If you want specific people to review your pull request, use the `@` notation in Github comments to mention that user, and request that he/she reviews your changes.
1. After all review is complete, combine all new commits into one squashed commit except when there are multiple contributors, and include the Jira number in the commit message. There are several ways to squash commits, but [here is one explanation from git-scm.com](https://git-scm.com/book/en/v2/Git-Tools-Rewriting-History#Squashing-Commits) and a simple example is illustrated below:

  If tracking upstream/devel-3 then run `git rebase -i`. Else run `git rebase -i upstream/devel-3`.  
  This command opens the text editor which lists the multiple commits:

  ```
  pick 67cd79b change1
  pick 6f98905 change2

  # Rebase e13748b..3463fbf onto e13748b (2 command(s))
  #
  # Commands:
  # p, pick = use commit
  # r, reword = use commit, but edit the commit message
  # e, edit = use commit, but stop for amending
  # s, squash = use commit, but meld into previous commit
  # f, fixup = like "squash", but discard this commit's log message
  # x, exec = run command (the rest of the line) using shell
  #
  # These lines can be re-ordered; they are executed from top to bottom.
  ```
  Squash 'change2' to 'change1' and save.

  ```
  pick 67cd79b change1
  squash 6f98905 change2
  ```
1. If there are multiple contributors in a pull request preserve individual attributions. Try to squash the commits to the minimum number of commits required to preserve attribution and the contribution to still be functionally correct.
1. Till the review is complete it may happen that working feature branch may diverge from `devel-3` substantially. Therefore, it is recommended to frequently merge `devel-3` to the branch being worked on by:
  * when the branch tracks upstream/devel-3  
  `git pull`
  * when the branch doesn't track upstream  
  `git pull upstream devel-3`
1. If a pull from `devel-3` results in a conflict then resolve it and commit the merge. This results in additional merge commits in the pull request. Following steps help to ensure that the final pull request contains just one commit:
  * Rename the original branch:  
  `git branch -m APEX-123.my-feature.squash`
  * Create a new branch (with the original name) from upstream/devel-3 that has latest changes:   
  `git checkout -b APEX-123.my-feature -t upstream/devel-3`
  * Squash merge the old branch which was renamed. When the new branch has the latest changes then this squash will result only in the changes that were made for the feature:  
  `git merge --squash APEX-123.my-feature.squash`
  * Commit the squash and force push it to the old feature remote branch so that the pull request is automatically updated:    
  `git commit -m "APEX-123 #comment added my-feature" `  
  `git push origin +APEX-123.my-feature`
  * Delete the extra squash branch:  
  `git branch -D APEX-123.my-feature.squash`

Thanks for contributing!

## Merging a Pull Request (committers)

1. Ensure that the basic requirements for a pull request are met. This includes:
  - Commit messages need to reference JIRA (pull requests will be linked to ticket)
  - Travis CI pull request build needs to pass
  - Ensure tests are added/modified for new features or fixes
  - Ensure appropriate JavaDoc comments have been added
1. To set up access to the ASF source repository, [follow these steps](https://git-wip-us.apache.org/#committers-getting-started). The ASF master repository is: `https://git-wip-us.apache.org/repos/asf/incubator-apex-core.git`
1. Use the git command line to pull in the changes from the pull requests. You can refer to the corresponding email that will be automatically sent to the `dev@apex.incubator.apache.org` mailing list to see the exact commands to merge the given pull request.
1. Once done with verification, push the changes to the ASF repository's `devel-3` branch. Within a few
seconds, the changes will propagate back to the github mirror and the pull requests be closed and marked merged automatically.
1. The `Fix version` field on the corresponding JIRA ticket needs to be set after pushing the changes.

**Note: since none of us has write access to the mirror, only the author of a pull request can close it if it was not merged.**
