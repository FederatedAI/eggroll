## Release 3.0.0

**Enhancements in the JVM Part:**
1. **Core Component Reconstruction:** The `cluster-manager` and `node-manager` components have been entirely rebuilt using Java, ensuring uniformity and enhanced performance.
2. **Transport Component Modification:** The `rollsite` transport component has been removed and replaced with the more efficient `osx` component.
3. **Improved Process Management:** Advanced logic has been implemented to manage processes more effectively, significantly reducing the risk of process leakage.
4. **Enhanced Data Storage Logic:** Data storage mechanisms have been refined for better performance and reliability.
5. **Concurrency Control Improvements:** We've upgraded the logic for concurrency control in the original components, leading to performance boosts.
6. **Visualization Component:** A new visualization component has been added for convenient monitoring of computational information.
7. **Refined Logging:** The logging system has been enhanced for more precise outputs, aiding in rapid anomaly detection.

**Upgrades in the Python Part:**
1. **Reconstruction of `roll_pair` and `egg_pair`:** These components now support serialization and partition methods controlled by the caller. Serialization safety is uniformly managed by the caller.
2. **Automated Cleanup of Intermediate Tables:** The issue of automatic cleaning for intermediate tables between federation and computing has been resolved, eliminating the need for extra operations by the caller.
3. **Unified Configuration Control:** A flexible configuration system is introduced, supporting direct pass-through, configuration files, and environment variables to cater to diverse requirements.
4. **Client-Side PyPI Installation:** Eggroll 3.0 supports easy installation via PyPI for clients.
5. **Optimized Log Configuration:** Callers can now customize log formats according to their needs.
6. **Code Structure Refinement:** The codebase has been streamlined for clarity, removing a substantial amount of redundant code.

